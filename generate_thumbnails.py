import boto3
import os
from dotenv import load_dotenv
import fitz
from PIL import Image
import io
import tempfile
from pathlib import Path
import logging
import time
from datetime import datetime
import ebooklib
from ebooklib import epub
from weasyprint import HTML
import requests

load_dotenv(".env")

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS clients
S3_CLIENT = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("S3_REGION")
)

dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("DYNAMODB_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

# Constants
EBOOK_BUCKET = "primary-school-ebook-data"
COMPRESSED_FOLDER = "compressed/"
THUMBNAILS_FOLDER = "thumbnails/"
EBOOK_TABLE = "ebook-store"

def generate_thumbnail_from_pdf(pdf_path, output_path):
    """Generate a thumbnail image from PDF file"""
    try:
        # Verify file exists
        if not os.path.exists(pdf_path):
            logger.error(f"PDF file not found: {pdf_path}")
            return False

        # Open PDF from file
        doc = fitz.open(pdf_path)
        
        if len(doc) > 0:
            # Get first page
            first_page = doc[0]
            # Render page to pixmap
            pix = first_page.get_pixmap(matrix=fitz.Matrix(2, 2))
            
            # Convert to PIL Image
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            
            # Resize if needed
            max_size = (800, 800)
            img.thumbnail(max_size, Image.Resampling.LANCZOS)
            
            # Save as JPEG
            img.save(output_path, "JPEG", quality=85)
            doc.close()
            return True
        else:
            logger.error("PDF has no pages")
            doc.close()
            return False
    except fitz.FileNotFoundError:
        logger.error(f"Could not open PDF file: {pdf_path}")
        return False
    except fitz.FileDataError:
        logger.error(f"Invalid or corrupted PDF file: {pdf_path}")
        return False
    except Exception as e:
        logger.error(f"Error generating thumbnail from PDF: {e}")
        if 'doc' in locals():
            doc.close()
        return False

def generate_thumbnail_from_epub(epub_path, output_path):
    """Generate a thumbnail image from EPUB file"""
    try:
        # Read EPUB
        book = epub.read_epub(epub_path)
        
        # Find cover image or first content page
        cover_found = False
        
        # Try to find cover image
        for item in book.get_items_of_type(ebooklib.ITEM_COVER):
            with open(output_path, 'wb') as f:
                f.write(item.content)
            cover_found = True
            break
        
        # If no cover, try first image
        if not cover_found:
            for item in book.get_items_of_type(ebooklib.ITEM_IMAGE):
                with open(output_path, 'wb') as f:
                    f.write(item.content)
                cover_found = True
                break
        
        # If still no cover, render first HTML page
        if not cover_found:
            for item in book.spine:
                item_id = item[0]
                item_obj = book.get_item_with_id(item_id)
                if item_obj is not None and item_obj.get_type() == ebooklib.ITEM_DOCUMENT:
                    content = item_obj.get_content().decode('utf-8')
                    
                    # Save HTML to temp file
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as temp_html:
                        temp_html.write(content)
                        html_path = temp_html.name
                    
                    # Convert to PDF
                    pdf_path = html_path.replace('.html', '.pdf')
                    HTML(filename=html_path).write_pdf(pdf_path)
                    
                    # Generate thumbnail from PDF
                    success = generate_thumbnail_from_pdf(pdf_path, output_path)
                    
                    # Clean up temporary files
                    os.unlink(html_path)
                    os.unlink(pdf_path)
                    
                    return success
        
        # Process the image if found
        if cover_found:
            # Optimize the image
            with Image.open(output_path) as img:
                # Convert to RGB if needed
                if img.mode in ('RGBA', 'P'):
                    img = img.convert('RGB')
                
                # Resize if needed
                max_size = (800, 800)
                img.thumbnail(max_size, Image.Resampling.LANCZOS)
                
                # Save optimized image
                img.save(output_path, "JPEG", quality=85)
            return True
        
        return False
    except Exception as e:
        logger.error(f"Error generating thumbnail from EPUB: {e}")
        return False

def update_dynamodb(file_key, thumbnail_url):
    """Update DynamoDB with thumbnail URL"""
    try:
        table = dynamodb.Table(EBOOK_TABLE)
        table.update_item(
            Key={'file_key': file_key},
            UpdateExpression='SET thumbnail = :thumbnail_url',
            ExpressionAttributeValues={':thumbnail_url': thumbnail_url}
        )
        logger.info(f"Updated DynamoDB for {file_key}")
        return True
    except Exception as e:
        logger.error(f"Error updating DynamoDB for {file_key}: {e}")
        return False

def process_ebooks():
    """Process all ebooks in the COMPRESSED_FOLDER"""
    tmp_dir = "temp/"
    Path(tmp_dir).mkdir(exist_ok=True)
    
    # Get list of ebooks from DynamoDB that don't have thumbnails
    table = dynamodb.Table(EBOOK_TABLE)
    
    try:
        # Scan for items without thumbnail field
        response = table.scan()
        items = response.get('Items', [])
        
        logger.info(f"Found {len(items)} items in DynamoDB")
        
        for item in items:
            file_key = item.get('file_key')
            
            # Skip if thumbnail already exists
            if 'thumbnail' in item and item['thumbnail']:
                logger.info(f"Thumbnail already exists for {file_key}, skipping")
                continue
            
            # Get file URL from DynamoDB
            file_url = item.get('url')
            if not file_url:
                logger.warning(f"No URL found for {file_key}, skipping")
                continue
            
            logger.info(f"Processing {file_key}")
            
            try:
                # Download file content from URL
                response = requests.get(file_url)
                
                # Store the file in /temp folder
                temp_file_path = os.path.join(tmp_dir, file_key)
                with open(temp_file_path, 'wb') as f:
                    f.write(response.content)
                
                # Create temporary file for thumbnail
                thumbnail_path = os.path.join(tmp_dir, f"{file_key}.jpg")
                
                # Generate thumbnail based on file type
                success = False
                if file_key.lower().endswith('.pdf'):
                    success = generate_thumbnail_from_pdf(temp_file_path, thumbnail_path)
                elif file_key.lower().endswith('.epub'):
                    success = generate_thumbnail_from_epub(temp_file_path, thumbnail_path)
                else:
                    logger.warning(f"Unsupported file type: {file_key}")
                    continue
                
                if success:
                    # Upload thumbnail to S3
                    thumbnail_key = f"{THUMBNAILS_FOLDER}{file_key.split('.')[0]}.jpg"
                    
                    with open(thumbnail_path, 'rb') as f:
                        S3_CLIENT.put_object(
                            Bucket=EBOOK_BUCKET,
                            Key=thumbnail_key,
                            Body=f,
                            ContentType='image/jpeg'
                        )
                    
                    # Generate thumbnail URL
                    thumbnail_url = f"https://{EBOOK_BUCKET}.s3.{os.getenv('S3_REGION')}.amazonaws.com/{thumbnail_key}"
                    
                    # Update DynamoDB
                    update_dynamodb(file_key, thumbnail_url)
                    
                    # Clean up
                    if os.path.exists(thumbnail_path):
                        os.remove(thumbnail_path)
                    
                    logger.info(f"Successfully processed {file_key}")
                else:
                    logger.warning(f"Failed to generate thumbnail for {file_key}")
            
            except Exception as e:
                logger.error(f"Error processing {file_key}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Error scanning DynamoDB: {e}")
    
    # Clean up temp directory
    if os.path.exists(tmp_dir):
        for file in os.listdir(tmp_dir):
            os.remove(os.path.join(tmp_dir, file))

if __name__ == "__main__":
    start_time = time.time()
    logger.info("Starting thumbnail generation process")
    process_ebooks()
    logger.info(f"Thumbnail generation completed in {time.time() - start_time:.2f} seconds")