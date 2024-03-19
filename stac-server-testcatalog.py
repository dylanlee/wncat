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
from datetime import datetime, timezone, timedelta
from shapely.geometry import Polygon, mapping, box
from pyproj import Transformer
from tempfile import TemporaryDirectory
from PIL import Image
import numpy as np
import boto3
import re
from datetime import date
from botocore.exceptions import NoCredentialsError, ClientError
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods

import stac_mod as sm

# set logging level for boto3
logging.basicConfig(level=logging.INFO)

########### collection specific defitions
#TODO these will get moved to their own files once you refactor so that every collections analysis and creation is happening in its own folder/namespace
def get_item_datetime(filename):
    # Regular expression to match the start and end datetime in the filename
    datetime_match = re.search(r"s(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2}).*?_e(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", filename)
    
    if datetime_match:
        start_year, start_month, start_day, start_hour, start_minute, start_second, \
        end_year, end_month, end_day, end_hour, end_minute, end_second = map(int, datetime_match.groups())

        start_datetime = datetime(start_year, start_month, start_day, start_hour, start_minute, start_second, tzinfo=timezone.utc)
        end_datetime = datetime(end_year, end_month, end_day, end_hour, end_minute, end_second, tzinfo=timezone.utc)
    else:
        # Default datetime if the filename doesn't match the expected format
        default_datetime = datetime(2023, 7, 7, 0, 0, tzinfo=timezone.utc)
        start_datetime = default_datetime
        end_datetime = default_datetime

    return start_datetime, end_datetime

# function that writes an updated stac collection file to s3 and loads into database
def update_collection(collection,collection_object_key,bucket_name,loader,s3):
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
    loader.load_collections(file=collection.self_href, insert_mode=Methods.upsert)


def get_subcollection(date, main_collection, loader, s3):
    """
    This function checks if a subcollection for a specific date exists.
j   If it does not, it creates the subcollection and returns it.
    """
    subcollection_key = f'collections/viirs-1-day/{date}/collection.json'
    subcollection_id = f"viirs-1-day-composite-{date.replace('/', '')}"
    subcollection_href = f'https://{bucket_name}.s3.amazonaws.com/{subcollection_key}'

    try:
        # Check if subcollection exists in S3
        s3.head_object(Bucket=bucket_name, Key=subcollection_key)
        # Download the existing subcollection JSON from S3
        response = s3.get_object(Bucket=bucket_name, Key=subcollection_key)
        subcollection_json = response['Body'].read()
        subcollection = pystac.Collection.from_dict(json.loads(subcollection_json))
        logging.info("Subcollection exists and loaded successfully.")
    except ClientError:
        # If subcollection does not exist, create it
        subcollection = pystac.Collection(
            id=subcollection_id,
            description=f"VIIRS 1-day composite flood water fraction product for {date}.",
            extent=main_collection.extent,
            title=f"VIIRS 1-day Composite for {date}",
            license='CC0-1.0',
            providers=main_collection.providers
        )
        subcollection.add_link(pystac.Link(rel="root", target=main_collection.get_self_href()))

        subcollection.add_link(pystac.Link(rel="parent", target=main_collection.get_self_href()))

        subcollection.stac_version = "1.0.0"

        main_collection.add_child(subcollection)
       
        # Manually set or update the 'self' link
        subcollection.remove_links('self')  # Remove existing 'self' link if any
        subcollection.add_link(pystac.Link(rel="self", target=subcollection_href))  # Add the new 'self' link

    return subcollection, subcollection_key

def generate_date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

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

########### Initialize viirs-1-day main composite collection
# Set switch to update or keep current collection
updateCollection = True

# Create an S3 client 
s3 = boto3.client('s3')

# Specify your bucket name
bucket_name = 'fim-public'

# Initialize the loader with your database 
db = PgstacDB(debug=True)
loader = Loader(db)

# Key for the collection object in the S3 bucket, within the "collections" folder
collection_object_key = 'collections/viirs-1-day/viirs-1-day.json'

# check if the collection object exists in the S3 bucket
try:
    s3.head_object(Bucket=bucket_name, Key=collection_object_key)
    object_exists = True
except ClientError as error:
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
    start_date = date(2012, 1, 21)  # Modify this date as per your data storage requirement
    yesterday_date = datetime.utcnow().date() - timedelta(days=1)

    # Convert the start_date to a datetime object with timezone info
    start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)

    # This is the top level viirs-1-day collection. It will host sub-collections where the main thing that is different
    # that the temporal extent will be the 24 hour period the composite was created in 0 Z.
    collection = pystac.Collection(
        id='viirs-1-day-composite',
        description='VIIRS 1-day composite flood water fraction product collection. Each days worth of products is organized as a subcollection of this collection. Please contact slia at gmu.edu for product specific questions.',
        title = "viirs-1-day-composite",
        keywords = ["VIIRS", "flood", "composite", "daily", "surface water"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[start_datetime, None]])
        ),
        license='CC0-1.0',
    )

    collection.add_link(pystac.Link(
        rel="related",
        target='https://waternode.ciroh.org/data-guide.html',
        title='VIIRS composite flood map entry in WPN data guide',
        media_type='text/html'
    ))

    # set stac version collection conforms to
    collection.stac_version = "1.0.0"

    collection.providers = [
        pystac.Provider(name="NOAA NESDIS", roles=["producer", "licensor"], url="https://www.nesdis.noaa.gov/"),
        pystac.Provider(name="VIIRS Flood Team at George Mason University", roles=["producer"], url="https://fhrl.vse.gmu.edu/"),
    ]

    # Set the collection's parent, root and self_href 
    collection.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{collection_object_key}')

    # write updated collection to s3 and upsert into pgstac
    update_collection(collection, collection_object_key, bucket_name,loader, s3)

########### add items to that days sub-collection
jpss_bucket_name = 'noaa-jpss'
for single_date in generate_date_range(start_date, yesterday_date):

    formatted_date = single_date.strftime("%Y/%m/%d")
    jpss_prefix = f'JPSS_Blended_Products/VFM_1day_GLB/TIF/{formatted_date}/'
    subcollection, subcollection_key = get_subcollection(single_date.strftime('%Y/%m/%d'), collection, loader, s3)

    # write subcollection to s3 and database update main collection to include new subcollection 
    update_collection(subcollection, subcollection_key, bucket_name,loader, s3)
    update_collection(collection, collection_object_key, bucket_name,loader, s3)

    tif_urls = list_tifs_in_bucket(jpss_bucket_name, jpss_prefix, s3)
    tmp_dir = tempfile.mkdtemp(dir='/home/dylan/wncat/tmpimgs')

    for link in tif_urls:
        # make netcdf link so can link to it in item as well
        netCDF_link = link.replace("TIF", "NetCDF")
        netCDF_link = netCDF_link[:-4] + ".nc"

        filename = link.split("/")[-1]
        base_filename = os.path.splitext(filename)[0]

        #extract date from filename
        start_datetime, end_datetime = get_item_datetime(filename)

        # get a datetime string for bucket object labels
        item_datetime_string = start_datetime.strftime('%Y-%m-%d')

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
        title = f"{truncated_id}_{single_date.strftime('%Y%m%d')}"

        item = pystac.Item(id=truncated_id[-3:],
                           geometry=footprint,
                           bbox=bbox.bounds,
                           collection = collection,
                           datetime = start_datetime, 
                           start_datetime = start_datetime,
                           end_datetime = end_datetime,
                           properties={
                                "title": title, 
                                "description" : 'VIIRS 1-day composite flood water fraction raster',
                                "processing level": "4",
                                "platform": "NPP, N20",
                                "instrument": "VIIRS",
                                "constellation": "JPSS",
                                "gsd": 350,
                                "license":'CC0-1.0',
                               })

        item.providers = [
            pystac.Provider(name="NOAA NESDIS", roles=["producer", "licensor"], url="https://www.nesdis.noaa.gov/"),
            pystac.Provider(name="VIIRS Flood Team at George Mason University", roles=["producer"], url="https://fhrl.vse.gmu.edu/"),
        ]


        # set stac version item conforms to
        item.stac_version = "1.0.0"

        # Enable the projection extension on the item
        ProjectionExtension.add_to(item)

        # Add projection information
        proj_ext = ProjectionExtension.ext(item)
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
                title="Cloud Optimized Geotiff",
                media_type=pystac.MediaType.COG
            )
        )


        # link out to tiff file on noaa jpss bucket
        item.add_asset(
            key='data',
            asset=pystac.Asset(
                href= netCDF_link,
                title="netCDF",
                media_type="application/netcdf"
            )
        )

        # add item this days subcollection
        subcollection.add_item(item)
       
        # Key for the item object in the S3 bucket
        item_key = f'items/viirs-1-day/{item.datetime.strftime("%Y/%m/%d")}/{item.id}.json'
        item.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{item_key}')
         
        # validate the item
        try:
            item.validate()
            print("The item is valid according to the STAC specification.")
        except Exception as e:
            print(f"Validation error: {e}")

        # Convert the item to a JSON string
        item_json = json.dumps(item.to_dict())

        # Write the JSON string to the S3 bucket
        s3.put_object(Body=item_json, Bucket=bucket_name, Key=item_key, ContentType='application/json')

        # insert/update the item in the database
        loader.load_items(file=item.self_href, insert_mode=Methods.upsert)

        # update subcollection
        update_collection(subcollection, subcollection_key, bucket_name, loader, s3)   

    # clean up the tmp_dir
    shutil.rmtree(tmp_dir)
    

