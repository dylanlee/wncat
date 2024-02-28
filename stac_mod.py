import requests
from bs4 import BeautifulSoup
import os
import json
import rasterio
import urllib.request
import pystac
from pyproj import Transformer
from datetime import datetime, timezone
from shapely.geometry import Polygon, mapping, box
from tempfile import TemporaryDirectory
from PIL import Image
import numpy as np
import boto3
import re
from datetime import date
from botocore.exceptions import NoCredentialsError

#Functions to help pull images off an htttp server
def fetch_page_content(url):
    """Fetch content from the given URL."""
    response = requests.get(url)
    return BeautifulSoup(response.text, "html.parser")

def extract_image_urls(base_url, soup, filter_strings):
    """Extract image URLs based on filter strings."""
    urls = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if all(s in href for s in filter_strings):
            full_url = os.path.join(base_url, href)
            urls.append(full_url)
    return urls

def download_images(urls, target_dir):
    """Download images from the given URLs."""
    for url in urls:
        filename = url.split('/')[-1]
        img_path = os.path.join(target_dir, filename)
        urllib.request.urlretrieve(url, img_path)
        print(f"Fetched {url}")

def get_bbox_and_footprint(raster):
    with rasterio.open(raster) as r:
        bounds = r.bounds
        bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        footprint = Polygon([
            [bounds.left, bounds.bottom],
            [bounds.left, bounds.top],
            [bounds.right, bounds.top],
            [bounds.right, bounds.bottom]
        ])
        # Get the CRS of the raster
        raster_crs = r.crs

        return (bbox, mapping(footprint), raster_crs)

# Function to transform bounding box coordinates
def transform_bbox_to_crs(bbox, src_crs, dst_crs):
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    minx, miny = transformer.transform(bbox.bounds[0], bbox.bounds[1])
    maxx, maxy = transformer.transform(bbox.bounds[2], bbox.bounds[3])
    return box(minx, miny, maxx, maxy)

# Create a thumbnail from downloaded image
def create_thumbnail(raster, thumbnail_path, size=(256, 256)):
    with rasterio.open(raster) as src:
        # Read the image data and get the profile
        img_data = src.read(1)
        profile = src.profile

        # Normalize the image data to 0-255
        img_data = (img_data - img_data.min()) / (img_data.max() - img_data.min()) * 255
        img_data = img_data.astype(np.uint8)

        # Create a PIL Image from the raster data
        pil_image = Image.fromarray(img_data)

        # Resize the image to the thumbnail size
        thumbnail = pil_image.resize(size, Image.ANTIALIAS)

        # Save the thumbnail
        thumbnail.save(thumbnail_path, format="PNG")

def delete_old_s3_files(bucket_name, prefix, start_date):
    """
    Delete files in the S3 bucket that have a filename date before the given start date.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix for the thumbnails in the S3 bucket.
        start_date (date): The start date to compare against.
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    
    for obj in bucket.objects.filter(Prefix=prefix):
        filename = obj.key
        # Extract date from the filename using regex
        match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
        if match:
            year, month, day = map(int, match.groups())
            file_date = date(year, month, day)
            if file_date < start_date:
                obj.delete()

def get_item_date(filename):
    # Extract the date from the filename
    date_match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
    if date_match:
        year, month, day = map(int, date_match.groups())
        item_datetime = datetime(year, month, day, tzinfo=timezone.utc)
    else:
        # Default datetime if the filename doesn't match the expected format
        item_datetime = datetime(2023, 7, 7, 0, 0, tzinfo=timezone.utc)

    return item_datetime

def upload_to_s3_with_retry(s3_client, file_path, bucket, key, max_retries=5, backoff_factor=1.5):
    attempt = 0
    while attempt < max_retries:
        try:
            s3_client.upload_file(file_path, bucket, key)
            return  # If upload succeeds, return from the function
        except NoCredentialsError:
            print('Credentials not available.')
            return
        except Exception as e:  # Catch other exceptions that might occur
            print(f"Error on attempt {attempt}: {e}")
            time.sleep(backoff_factor ** attempt)  # Exponential backoff
            attempt += 1
    raise Exception(f"Failed to upload {file_path} to s3://{bucket}/{key} after {max_retries} retries")

