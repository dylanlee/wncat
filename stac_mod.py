import requests
from bs4 import BeautifulSoup
import os
import json
import rasterio
from rasterio.transform import from_bounds
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
import time

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

def create_preview(raster, preview_path, size=(256, 256)):
    with rasterio.open(raster) as src:
        # Read the single band
        img_data = src.read(1)
        
        # Retrieve the colormap from the raster
        colormap = src.colormap(1)
        
        # Initialize an empty RGB(A) array
        img_data_rgba = np.zeros((img_data.shape[0], img_data.shape[1], 4), dtype=np.uint8)
        
        # Apply the colormap to create an RGBA representation
        for index, color in colormap.items():
            mask = img_data == index
            img_data_rgba[mask] = color  # Color is expected to be RGBA

        # Convert the RGBA array to a PIL Image
        pil_image = Image.fromarray(img_data_rgba, 'RGBA')

        # Calculate new size to maintain aspect ratio
        img_width, img_height = pil_image.size
        max_width, max_height = size
        scale = min(max_width/img_width, max_height/img_height)

        # New size with maintained aspect ratio
        new_width = int(img_width * scale)
        new_height = int(img_height * scale)

        # Resize the image with new size
        preview = pil_image.resize((new_width, new_height), Image.ANTIALIAS)

        # Save the preview
        preview.save(preview_path, format="PNG")

def delete_old_s3_files(bucket_name, prefix, start_date):
    """
    Delete files in the S3 bucket that have a filename date before the given start date.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix for the object in the S3 bucket.
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

def calculate_cover_percent(img_path,val):
    try:
        with rasterio.open(img_path) as src:
            # Read the first band
            band1 = src.read(1)
            
            # Count pixels with value x (30 for cloudy pixels and 20 for snow)
            val_pixels = np.sum(band1 == val)
            
            # Total number of pixels in the image
            total_pixels = band1.size
            
            # Compute the percentage of the image under cloud cover
            cover_percentage = np.round((val_pixels / total_pixels) * 100)
            
        return cover_percentage
    except Exception as e:
        print(f"An error occurred calculating cloud cover: {e}")
        return None
