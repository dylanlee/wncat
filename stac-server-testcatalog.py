import tempfile
import logging
import requests
from bs4 import BeautifulSoup
import os
import json
import rasterio
import urllib.request
import pystac
from pystac.extensions.projection import ProjectionExtension
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
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods

import stac_mod as sm

# set logging level for boto3
logging.basicConfig(level=logging.INFO)

########### collection specific defitions
#TODO these will get moved to their own files once you refactor so that every collections analysis and creation is happening in its own folder/namespace
def get_item_date(filename):
    # Extract the date from the filename, assuming the date follows 's'
    date_match = re.search(r"s(\d{4})(\d{2})(\d{2})", filename)
    if date_match:
        year, month, day = map(int, date_match.groups())
        item_datetime = datetime(year, month, day, tzinfo=timezone.utc)
    else:
        # Default datetime if the filename doesn't match the expected format
        item_datetime = datetime(2023, 7, 7, 0, 0, tzinfo=timezone.utc)

    return item_datetime

########### Initialize viirs-1-day composite collection
# Set switch to update or keep current collection
updateCollection = True

# Create an S3 client 
s3 = boto3.client('s3')

# Specify your bucket name
bucket_name = 'fim-public'

# Key for the collection object in the S3 bucket, within the "collections" folder
collection_object_key = 'collections/viirs-1-day.json'

# check if the collection object exists in the S3 bucket
try:
    s3.head_object(Bucket=bucket_name, Key=collection_object_key)
    object_exists = True
except s3.exceptions.NoSuchKey:
    object_exists = False

if object_exists and not updateCollection:
    print("The collection exists and updateCollection is False. Skipping creation and will only update items.")
    # Download the existing collection JSON from S3
    response = s3.get_object(Bucket=bucket_name, Key=collection_object_key)
    collection_json = response['Body'].read()
    collection_dict = json.loads(collection_json)
    collection = pystac.Collection.from_dict(collection_dict)
    print("Existing collection loaded successfully.")

else:
    print("Proceeding with collection creation and upserting...")

    # date from which data for the collection begins
    start_date = date(2012, 1, 20)  # Modify this date as per your data storage requirement
    # Convert the start_date to a datetime object with timezone info
    start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)

    # This is the top level viirs-1-day collection. It will host sub-collections where the main thing that is different
    # that the temporal extent will be the 24 hour period the composite was created in 0 Z.
    collection = pystac.Collection(
        id='viirs-1-day-composite',
        description='VIIRS 1-day composite flood water fraction product collection. Please contact slia at gmu.edu for product specific questions.',
        title = "viirs-1-day-composite",
        keywords = ["VIIRS", "flood", "composite", "daily", "surface water"],
        extensions = ["projection"]
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[start_datetime, None]])
        ),
        license='CC0-1.0',
    )

    collection.add_link(pystac.Link(
        rel=pystac.RelType.RELATED,
        target='https://waternode.ciroh.org/data-guide.html',
        title='VIIRS composite flood map entry in WPN data guide',
        media_type='text/html'
    ))

    # set stac version collection conforms to
    collection.stac_version = "1.0.0"

    collection.providers = [
        pystac.Provider(name="NOAA NESDIS", roles=["producer", "licensor"], url="https://www.nesdis.noaa.gov/"),
        pystac.Provider(name="VIIRS Flood Team at George Mason University", roles=["producer"], url="https://www.nesdis.noaa.gov/"),
    ]

    # Set the collection's parent, root and self_href 
    collection.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{collection_object_key}')

    # Convert the collection to a JSON string
    collection_json = json.dumps(collection.to_dict())

    # Write the collection JSON string to the S3 bucket
    s3.put_object(Body=collection_json, Bucket=bucket_name, Key=collection_object_key, ContentType='application/json')

    # validate the collection
    try:
        collection.validate()
        print("The collection is valid according to the STAC specification.")
    except Exception as e:
        print(f"Validation error: {e}")

    # upsert the new/modified collection item to the catalog

    # Initialize the loader with your database 
    db = PgstacDB(debug=True)
    loader = Loader(db)

    # (re)-Load your collection into the postgres database
    loader.load_collections(file=collection.self_href, insert_mode=Methods.upsert)

########### add items to collection

# TODO: add a bit of code that queries the collection and only creates items for after the latest date
# of the collection so that items don't get recreated.
    
### Testing with one day of the s3 bucket data
jpss_bucket_name = 'noaa-jpss'
jpss_prefix = 'JPSS_Blended_Products/VFM_1day_GLB/TIF/2012/01/21/'

# 3rd argument is a boto3 s3 client instance
def list_tifs_in_bucket(bucket, prefix, client):
    """List all TIFF files in the bucket under the given prefix."""
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    tif_urls = []
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.tif'):
                tif_url = f"https://{bucket}.s3.amazonaws.com/{obj['Key']}"
                tif_urls.append(tif_url)
    return tif_urls

tif_urls = list_tifs_in_bucket(jpss_bucket_name, jpss_prefix, s3)
tmp_dir = tempfile.mkdtemp(dir='/home/dylan/wncat/tmpimgs')

for link in tif_urls:
    # make netcdf link so can link to it in item as well
    netCDF_link = tif_url.replace("TIF", "NetCDF")

    filename = link.split("/")[-1]
    base_filename = os.path.splitext(filename)[0]

    #extract date from filename
    item_datetime = get_item_date(filename)
    item_datetime_string = item_datetime.strftime('%Y-%m-%d')

    img_path = os.path.join(tmp_dir, filename)

    # Download the TIFF from the link
    response = requests.get(link)
    response.raise_for_status()  # Raises an HTTPError if the response was an unsuccessful status code

    # Write the content of the response to a file in the temporary directory
    with open(img_path, 'wb') as file:
        file.write(response.content)

    # get information about image
    bbox, footprint, raster_crs = sm.get_bbox_and_footprint(img_path)

    thumbnail_path = os.path.join(tmp_dir, f"thumbnail_{base_filename}.png")
    sm.create_preview(img_path, thumbnail_path)

    # Upload thumnail to s3
    try:
        sm.upload_to_s3_with_retry(s3, thumbnail_path, bucket_name, f'thumbnails/viirs-1-day/{item_datetime_string}/{base_filename}.png')
        s3_thumbnail_url = f"https://{bucket_name}.s3.amazonaws.com/thumbnails/viirs-1-day/{item_datetime_string}/{base_filename}.png"
    except NoCredentialsError:
        print('Credentials not available.')
    
    # create overview. A cog of the original image is <1 mb which is fine
    overview_path = os.path.join(tmp_dir, f"overview_{filename}")
    output_profile = cog_profiles.get("deflate")
    cog_translate(img_path, overview_path, output_profile)

    # Upload overview to s3
    try:
        sm.upload_to_s3_with_retry(s3, overview_path, bucket_name, f"overviews/viirs-1-day/{item_datetime_string}/{filename}")
        s3_overview_url = f"https://{bucket_name}.s3.amazonaws.com/overviews/viirs-1-day/{item_datetime_string}/{filename}"
    except NoCredentialsError:
        print('Credentials not available.') 

    truncated_id = filename.split("_")[0]

    item = pystac.Item(id=truncated_id,
                       geometry=footprint,
                       bbox=bbox.bounds,
                       collection = collection,
                       datetime=item_datetime,
                       properties={
                            "crs" : str(raster_crs),
                            "processing level": "4"
                            "satellite": "NPP, N20",
                            "instrument": "VIIRS",
                            "constellation": "JPSS",
                            "gsd": "350 m"
                           })

    item.providers = [
        pystac.Provider(name="NOAA NESDIS", roles=["producer", "licensor"], url="https://www.nesdis.noaa.gov/"),
        pystac.Provider(name="VIIRS Flood Team at George Mason University", roles=["producer"], url="https://www.nesdis.noaa.gov/"),
    ]


    # set stac version item conforms to
    collection.stac_version = "1.0.0"

    # Enable the projection extension on the item
    ProjectionExtension.add_to(item)

    # Add projection information
    proj_ext = ProjectionExtension()
    proj_ext.epsg = 4326

    # Add thumbnail asset
    item.add_asset(
        key='thumbnail',
        asset=pystac.Asset(
            href=s3_thumbnail_url,
            title="Thumbnail Image",
            media_type='image/png'
        )
    )

    # Add thumbnail asset
    item.add_asset(
        key='image',
        asset=pystac.Asset(
            href= s3_overview_url,
            title="cloud optimized geotiff",
            media_type=pystac.MediaType.COG
        )
    )


    # link out to tiff file on noaa jpss bucket
    item.add_asset(
        key='image',
        asset=pystac.Asset(
            href= netCDF_link,
            title="netCDF",
            media_type="application/netcdf"
        )
    )

    # add item this days collection
    collection.add_item(item)

    # Key for the object in the S3 bucket, inside the "items" folder
    object_key = f'items/viirs-1-day/{item_datetime_string}/{item.id}.json'

    # Set the item's self_href to the S3 URL
    item.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{object_key}')   
 
    # validate the item
    try:
        item.validate()
        print("The item is valid according to the STAC specification.")
    except Exception as e:
        print(f"Validation error: {e}")

    # Convert the item to a JSON string
    item_json = json.dumps(item.to_dict())

    # Write the JSON string to the S3 bucket
    s3.put_object(Body=item_json, Bucket=bucket_name, Key=object_key, ContentType='application/json')

    # insert/update the item in the database
    loader.load_items(file=item.self_href, insert_mode=Methods.upsert)

    # clean up the tmp_dir
    for item in os.listdir(tmp_dir):
        item_path = os.path.join(tmp_dir, item)
        if os.path.isfile(item_path) or os.path.islink(item_path):
            os.remove(item_path)  # Remove the file or link
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)  # Remove the directory and all its contents
