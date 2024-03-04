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
import datetime
from datetime import date
from botocore.exceptions import NoCredentialsError
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

import stac_mod as sm

# Create an S3 client 
s3 = boto3.client('s3')

#### Load in previous catalog and collection from bucket
bucket_name = 'fim-public'
catalog_object_key = 'catalog.json'
collection_object_key = 'collections/viirs-1-day.json'

catalog_data = s3.get_object(Bucket=bucket_name, Key=catalog_object_key)
catalog_content = catalog_data['Body'].read().decode('utf-8')
catalog = pystac.Catalog.from_dict(json.loads(catalog_content))

collection_data = s3.get_object(Bucket=bucket_name, Key=collection_object_key)
collection_content = collection_data['Body'].read().decode('utf-8')
collection = pystac.Collection.from_dict(json.loads(collection_content))

#### delete catalog self_href and parent child relationships. Doing this because item addition gets confused otherwise.

catalog.remove_links('self')
catalog.remove_links('parent')
catalog.remove_links('child')

# Remove the self_href, parent, and child relationships for collection
collection.remove_links('self')
collection.remove_links('parent')
collection.remove_links('child')

########### download images and add them to collection
base_url = "https://floodlight.ssec.wisc.edu/composite/"

soup = sm.fetch_page_content(base_url)
urls = sm.extract_image_urls(base_url, soup, ["composite1", ".tif"])
# Extract all item IDs from the collection
item_ids = {item.id for item in collection.get_all_items()}

# Extract date strings from item IDs
item_dates = {re.search(r"(\d{8})", item_id).group(1) for item_id in item_ids if re.search(r"(\d{8})", item_id)}

print("script running on")
current_datetime = datetime.datetime.now()
print(current_datetime)
print("Current item in catalog span the dates:")
print(item_dates)

# Filter out the URLs where the date string matches any of the item dates
filtered_urls = [url for url in urls if re.search(r"(\d{8})", url.split('/')[-1]).group(1) not in item_dates]

print("candidate new URLs are:")
print(filtered_urls)

# Update the urls list
urls = filtered_urls

# Bounding boxes for the Continental US, Hawaii, and Alaska in WGS 1984 CRS 
continental_us_bbox = box(-125.001650, 24.396308, -66.934570, 49.384358)
hawaii_bbox = box(-178.443593, 18.865460, -154.806773, 28.517269)
alaska_bbox = box(-178, 51.214183, -140, 71.538800)

# Store all bounding boxes and footprints in a dictionary
bbox_and_footprints = {}

with TemporaryDirectory(dir='/home/dylan/wncat/tmpimgs') as tmp_dir:
    sm.download_images(urls, tmp_dir)

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

                # Convert the TIFF to a COG
                cog_path = os.path.join(tmp_dir, f"cog_{filename}")
                output_profile = cog_profiles.get("deflate")
                cog_translate(img_path, cog_path, output_profile)

                # Upload the COG to S3 in the 'assets' folder
                s3_cog_key = f'assets/cog_{filename}'
                sm.upload_to_s3_with_retry(s3, cog_path, bucket_name, s3_cog_key)

                # Upload thumnail to s3
                try:
                    sm.upload_to_s3_with_retry(s3, thumbnail_path, bucket_name, f'thumbnails/{filename}.png')
                    s3_thumbnail_url = f"https://{bucket_name}.s3.amazonaws.com/thumbnails/{filename}.png"
                except NoCredentialsError:
                    print('Credentials not available.')

                # Store the bounds, footprint, and COG S3 URL
                s3_cog_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_cog_key}"
                bbox_and_footprints[filename] = (bbox.bounds, footprint, s3_thumbnail_url, s3_cog_url)
# Create STAC items from all the tifs in the temporary directory
# List to hold all the STAC items
stac_items = []

# Loop over the bbox_and_footprints dictionary
for filename, (bbox, footprint, thumbnail_url,cog_url) in bbox_and_footprints.items():
    #extract date from filename
    item_datetime = sm.get_item_date(filename)

    item = pystac.Item(id=filename,
                       geometry=footprint,
                       bbox=bbox,
                       collection = collection,
                       datetime=item_datetime,
                       properties={})

    # Add thumbnail asset
    item.add_asset(
        key='thumbnail',
        asset=pystac.Asset(
            href=thumbnail_url,
            media_type='image/png'
        )
    )

    # add COG
    item.add_asset(
        key='image',
        asset=pystac.Asset(
            href=cog_url,
            media_type=pystac.MediaType.GEOTIFF
        )
    )

    # Append the new item to the list
    stac_items.append(item)

for item in stac_items:
    
    # Key for the object in the S3 bucket, inside the "items" folder
    object_key = f'items/{item.id}.json'
 
    # add item to collection
    collection.add_item(item)

    # Set the item's self_href to the S3 URL
    item.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{object_key}')   
    # Convert the item to a JSON string
    item_json = json.dumps(item.to_dict())

    # Write the JSON string to the S3 bucket
    s3.put_object(Body=item_json, Bucket=bucket_name, Key=object_key, ContentType='application/json')

# Restore the self_href, parent, and child relationships
catalog.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{catalog_object_key}')
collection.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{collection_object_key}')


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

