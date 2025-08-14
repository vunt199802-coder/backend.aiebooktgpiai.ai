import boto3
from boto3.dynamodb.conditions import Attr
import os
from PIL import Image
import requests
from io import BytesIO
import logging
from dotenv import load_dotenv
import fitz
import hashlib
import unicodedata
from pathlib import Path
import re

load_dotenv(".env")

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('generate_thumbnail.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# DynamoDB and S3 clients
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("DYNAMODB_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("S3_REGION")
)

INDEXED_BUCKET = "primary-school-ebook-data"
THUMBNAIL_PREFIX = "thumbnails/"

def normalize_filename(filename):
        """Normalize filename while preserving Unicode characters."""
        # Normalize Unicode (NFKC form combines compatibility characters)
        normalized = unicodedata.normalize('NFKC', filename)
        
        # Replace problematic characters but keep Unicode letters and numbers
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', normalized)
        
        # Ensure the filename isn't too long
        if len(safe_filename.encode('utf-8')) > 255:
            # Generate a short hash of the original name
            name_hash = hashlib.md5(filename.encode('utf-8')).hexdigest()[:8]
            extension = Path(safe_filename).suffix
            safe_filename = f"{name_hash}{extension}"
            
        return safe_filename

# Function to generate thumbnail
def generate_thumbnail(pdf_url, book_id):
    try:
        book_id = normalize_filename(book_id)
        # Download PDF
        response = requests.get(pdf_url)
        print("==== response", response.status_code)
        if response.status_code == 200:
            # Convert PDF to thumbnail based on first page
            try:
                pdf = fitz.open(stream=BytesIO(response.content), filetype="pdf")  # Specify filetype
            except Exception as e:
                logger.error(f'Error opening PDF for book ID: {book_id}: {e}')
                return None
            first_page = pdf[0]
            
            pix = first_page.get_pixmap()
            # Get book name without extension
            book_name = os.path.splitext(book_id)[0].replace('/', '-')
            # Save the pixmap as a JPEG image
            thumbnail = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            thumbnail.save(f'{book_name}.jpg')  # Save the thumbnail

            print("==== saved on local")

            s3.upload_file(
                f"{book_name}.jpg",
                INDEXED_BUCKET,
                f"{THUMBNAIL_PREFIX}{book_name}.jpg",
                ExtraArgs={'ContentType': 'image/jpeg'}
            )

            print("==== saved on s3")

            os.remove(f"{book_name}.jpg")

            print("==== removed from local")
            
            # s3.put_object(Bucket='thumbnails', Key=f'{book_id}.jpg', Body=thumbnail_buffer.getvalue())
            logger.info(f'Thumbnail generated for book ID: {book_id}')
            # return f'{THUMBNAIL_PREFIX}{book_name}.jpg'
            return f"https://{INDEXED_BUCKET}.s3.{os.getenv('S3_REGION')}.amazonaws.com/{THUMBNAIL_PREFIX}{book_name}.jpg"
        
        else:
            logger.error(f'Failed to download PDF for book ID: {book_id}')
            return None
    except Exception as e:
        logger.error(f'Error generating thumbnail for book ID: {book_id}: {e}')
        return None

# Function to update DynamoDB with thumbnail URL
def update_dynamodb(book_id, thumbnail_url):
    try:
        table = dynamodb.Table('ebooks')
        table.update_item(
            Key={'file_key': book_id},
            UpdateExpression='SET #thumbnail = :val1',
            ExpressionAttributeNames={'#thumbnail': 'thumbnail'},
            ExpressionAttributeValues={':val1': thumbnail_url}
        )
        logger.info(f'Thumbnail URL updated for book ID: {book_id}')
    except Exception as e:
        logger.error(f'Error updating DynamoDB for book ID: {book_id}: {e}')

# Main function
def main():
    # Load all rows from DynamoDB table - ebooks
    try:
        table = dynamodb.Table('ebooks')
        
        # Scan the table to get all items
        response = table.scan(
            FilterExpression=Attr('thumbnail').not_exists()
        )
        total_rows = len(response['Items'])
        logger.info(f'Total rows loaded: {total_rows}')
        
        # Process each row
        for item in response['Items']:
            try:
                book_id = item['file_key']
                
                pdf_url = item['url']
                print('===== pdf_url', pdf_url, book_id)
                thumbnail_url = generate_thumbnail(pdf_url, book_id)
                if thumbnail_url:
                    update_dynamodb(book_id, thumbnail_url)
            except Exception as e:
                logger.error(f'Error processing book ID: {book_id}: {e}')
                continue
    except Exception as e:
        logger.error(f'Error loading rows from DynamoDB: {e}')

if __name__ == '__main__':
    main()
