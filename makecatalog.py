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
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

import stac_mod as sm

########### Initialize catalog and collection
# date from which data for the collection begins
start_date = date(2023, 8, 24)  # Modify this date as per your data storage requirement

# clean up stuff on s3 bucket before the catalog start_date

# Create an S3 client 
s3 = boto3.client('s3')

# Specify your bucket name
bucket_name = 'fim-public'

# Delete thumbnails that are before a given start date. This is to throttle the total size of collections
prefix = "thumbnails/"  # Modify this prefix if needed
sm.delete_old_s3_files(bucket_name, prefix, start_date)

# Delete old items
prefix = "items/"  # Modify this prefix if needed
sm.delete_old_s3_files(bucket_name, prefix, start_date)

# Delete old images
prefix = "assets/"  # Modify this prefix if needed
sm.delete_old_s3_files(bucket_name, prefix, start_date)

# Convert the start_date to a datetime object with timezone info
start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
catalog = pystac.Catalog(id='Water Prediction Node', description='The geospatial asset catalog of the Water Prediction Node.')

# viirs collection
collection = pystac.Collection(
    id='viirs-1-day',
    description='VIIRS 1-day composite flood maps collection',
    extent=pystac.Extent(
        spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent([[start_datetime, None]])
    ),
    license='public domain'
)

# Key for the catalog object in the S3 bucket
catalog_object_key = 'catalog.json'

# Key for the collection object in the S3 bucket, within the "collections" folder
collection_object_key = 'collections/viirs-1-day.json'

# Set the collection's self_href to the S3 URL
collection.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{collection_object_key}')

# List all the item JSON files from the S3 bucket's "items" folder
response = s3.list_objects_v2(Bucket=bucket_name, Prefix='items/')

# Check if the response contains 'Contents' key
if 'Contents' in response:
    for content in response['Contents']:
        object_key = content['Key']
        
        # Download the item JSON
        item_data = s3.get_object(Bucket=bucket_name, Key=object_key)
        item_content = item_data['Body'].read().decode('utf-8')
        
        # Parse the JSON content into a pystac.Item object
        item_dict = json.loads(item_content)
        item = pystac.Item.from_dict(item_dict)
        
        # Add the item to the collection
        collection.add_item(item)
        item.set_self_href(f'https://{bucket_name}.s3.amazonaws.com/{object_key}')

###### write catalog and collection. Be aware that messing with the sequencing of relationship assignments here or above might break catalog creation code!!! So have a commit to come back to.

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


