import requests
from bs4 import BeautifulSoup
import os
import json
import rasterio
import urllib.request
import pystac
from datetime import datetime, timezone
from shapely.geometry import Polygon, mapping, box
from pyproj import Transformer
from tempfile import TemporaryDirectory
from PIL import Image
import numpy as np
import boto3
import re
from datetime import date
from botocore.exceptions import NoCredentialsError

import stac_mod as sm


########### Initialize catalog and collection

# Create an S3 client 
s3 = boto3.client('s3')

# Specify your bucket name
bucket_name = 'fim-public'

catalog = pystac.Catalog(id='Water Prediction Node', description='The geospatial asset catalog of the Water Prediction Node.')

# viirs collection
collection = pystac.Collection(
    id='viirs-1-day',
    description='VIIRS 1-day composite flood maps collection',
    extent=pystac.Extent(
        spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent([[datetime(2023, 7, 7, 0, 0, tzinfo=timezone.utc), None]])
    ),
    license='public domain'
)

# Key for the catalog object in the S3 bucket
catalog_object_key = 'catalog.json'

# Key for the collection object in the S3 bucket, within the "collections" folder
collection_object_key = 'collections/collection.json'

# Set the collection's self_href to the S3 URL
collection.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{collection_object_key}')

########### download images and add them to collection
base_url = "https://floodlight.ssec.wisc.edu/composite/"

soup = sm.fetch_page_content(base_url)
urls = sm.extract_image_urls(base_url, soup, ["composite1", ".tif"])

# Bounding boxes for the Continental US, Hawaii, and Alaska in WGS 1984 CRS 
continental_us_bbox = box(-125.001650, 24.396308, -66.934570, 49.384358)
hawaii_bbox = box(-178.443593, 18.865460, -154.806773, 28.517269)
alaska_bbox = box(-178, 51.214183, -140, 71.538800)

# Store all bounding boxes and footprints in a dictionary
bbox_and_footprints = {}

with TemporaryDirectory() as tmp_dir:
    sm.download_images(urls[:10], tmp_dir)
    print(f"Images saved in {tmp_dir}")

    # Iterate through all tif files in the temporary directory
    for filename in os.listdir(tmp_dir):
        if filename.endswith('.tif'):
            img_path = os.path.join(tmp_dir, filename)
            bbox, footprint, raster_crs = sm.get_bbox_and_footprint(img_path)
            # Convert the US bbox's to the same crs as the raster_crs
            transformer = Transformer.from_crs("EPSG:4326", raster_crs, always_xy=True)  

            continental_us_bbox_transformed = box(*transformer.transform_bounds(*continental_us_bbox.bounds))
            hawaii_bbox_transformed = box(*transformer.transform_bounds(*hawaii_bbox.bounds))
            alaska_bbox_transformed = box(*transformer.transform_bounds(*alaska_bbox.bounds))

            # Check if the bounding box intersects with the Continental US, Hawaii, or Alaska
            if bbox.intersects(continental_us_bbox_transformed) or bbox.intersects(hawaii_bbox_transformed) or bbox.intersects(alaska_bbox_transformed):
                # Create thumbnail
                thumbnail_path = os.path.join(tmp_dir, f"thumbnail_{filename}.png")
                sm.create_thumbnail(img_path, thumbnail_path)

                # Upload to S3
                try:
                    s3.upload_file(thumbnail_path, bucket_name, f'thumbnails/{filename}.png')
                    # Make sure to replace this with the actual URL template of your S3 bucket
                    s3_thumbnail_url = f"https://{bucket_name}.s3.amazonaws.com/thumbnails/{filename}.png"
                except NoCredentialsError:
                    print('Credentials not available.')

                # Store the bounds, footprint, and thumbnail path
                bbox_and_footprints[filename] = (bbox.bounds, footprint, s3_thumbnail_url)

# date from which data for the collection begins
start_date = date(2023, 8, 19)  # Modify this date as per your data storage requirement

# Delete thumbnails that are before a given start date. This is to throttle the total size of collections
prefix = "thumbnails/"  # Modify this prefix if needed
sm.delete_old_s3_files(bucket_name, prefix, start_date)

# Delete old items
prefix = "items/"  # Modify this prefix if needed
sm.delete_old_s3_files(bucket_name, prefix, start_date)

# Create STAC items from all the tifs in the temporary directory
# List to hold all the STAC items
stac_items = []

# Loop over the bbox_and_footprints dictionary
for filename, (bbox, footprint, thumbnail_url) in bbox_and_footprints.items():
    item = pystac.Item(id=filename,
                       geometry=footprint,
                       bbox=bbox,
                       collection = collection,
                       datetime=datetime(2023, 7, 7, 0, 0, tzinfo=timezone.utc),
                       properties={})

    # Add thumbnail asset
    item.add_asset(
        key='thumbnail',
        asset=pystac.Asset(
            href=thumbnail_url,
            media_type='image/png'
        )
    )

    # Append the new item to the list
    stac_items.append(item)

# Add the first 10 image assets from the URLs list
for item, url in zip(stac_items, urls[:10]):
    item.add_asset(
        key='image',
        asset=pystac.Asset(
            href=url.strip(),
            media_type=pystac.MediaType.GEOTIFF
        )
    )

for item in stac_items:
    print("item added. now writing json")
    # Key for the object in the S3 bucket, inside the "items" folder
    object_key = f'items/{item.id}.json'
 
    # add item to collection
    collection.add_item(item)

    # Set the item's self_href to the S3 URL
    item.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{object_key}')   
    # Convert the item to a JSON string
    item_json = json.dumps(item.to_dict())
    print(item_json)

    # Write the JSON string to the S3 bucket
    s3.put_object(Body=item_json, Bucket=bucket_name, Key=object_key, ContentType='application/json')

###### write catalog and collection

# Set the catalog's self_href to the S3 URL
catalog.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{catalog_object_key}')

# Set the catalog's parent to the stac-browser home
catalog_parent = pystac.Link(rel="parent", target=f'https://{bucket_name}.s3.amazonaws.com/{catalog_object_key}', media_type="application/json")
catalog.add_link(catalog_parent)

# add a parent link to the collection
collection_parent= pystac.Link(rel="parent", target=f'https://{bucket_name}.s3.amazonaws.com/{catalog_object_key}',media_type="application/json")
collection.add_link(collection_parent)

# Add the viirs collection to the catalog
catalog_child = pystac.Link(rel="child", target=f'https://{bucket_name}.s3.amazonaws.com/{collection_object_key}',media_type="application/json")
catalog.add_link(catalog_child)

# Convert the catalog to a JSON string
catalog_json = json.dumps(catalog.to_dict())

# Write the catalog JSON string to the S3 bucket
s3.put_object(Body=catalog_json, Bucket=bucket_name, Key=catalog_object_key, ContentType='application/json')

# Convert the collection to a JSON string
collection_json = json.dumps(collection.to_dict())

# Write the collection JSON string to the S3 bucket
s3.put_object(Body=collection_json, Bucket=bucket_name, Key=collection_object_key, ContentType='application/json')


