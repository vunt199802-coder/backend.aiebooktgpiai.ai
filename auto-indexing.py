from openai import OpenAI
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from ai.langchain import text_embedding
from routers.ebooks import get_image_description, encode_image_pdf
import fitz 
from io import BytesIO
import requests
import ebooklib
from ebooklib import epub
from weasyprint import HTML
from pathlib import Path

load_dotenv(".env")

# Assuming BUCKET_NAME and speechFile_name are defined elsewhere
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

client = OpenAI()

EBOOK_BUCKET = "primary-school-ebook-data"
COMPRESSED_FOLDER = "compressed/"
THUMBFILES_FOLDER = "thumbfiles/"
EBOOK_TABLE = "ebook-store"

def extract_content_from_epub(epub_path):
    print(f"Extracting content from EPUB: {epub_path}")
    # Read EPUB
    book = epub.read_epub(epub_path)
    
    pages = []
    images = {}
    
    # First, collect all images
    for item in book.get_items_of_type(ebooklib.ITEM_IMAGE):
        images[item.id] = item.content
    
    # Process spine items in order to maintain book structure
    for item in book.spine:
        item_id = item[0]
        item_obj = book.get_item_with_id(item_id)
        if item_obj is not None and item_obj.get_type() == ebooklib.ITEM_DOCUMENT:
            content = item_obj.get_content().decode('utf-8')
            # Keep the full HTML content including images
            pages.append({
                'html': content,
                'images': images
            })
    
    print(f"Found {len(pages)} pages with content")
    return pages

def process_files():
    tmp_dir = "temp/"
    Path(tmp_dir).mkdir(exist_ok=True)
    table = dynamodb.Table(EBOOK_TABLE)

    # List objects in the uploads directory
    response = S3_CLIENT.list_objects_v2(Bucket=EBOOK_BUCKET, Prefix=COMPRESSED_FOLDER)
    print(f"Count of ebooks on S3: {len(response.get('Contents', []))}")
    for obj in response.get('Contents', []):
        original_file_key = obj['Key']
        print('= original_file_key', original_file_key)
        file_key = original_file_key.split('/')[-1].replace(' ', '_')
        if not file_key:
            continue
    
        # Check if the file already exists in DynamoDB
        response = table.get_item(Key={'file_key': file_key})
        if 'Item' in response:
            print(f"File {file_key} already exists in DynamoDB, skipping upload and embedding.")
            continue

        tmp_file_path = os.path.join(tmp_dir, file_key.replace(' ', '_'))

        print(f"Downloading file: {file_key}...")
        # Download the file
        with open(tmp_file_path, 'wb') as f:
            S3_CLIENT.download_fileobj(EBOOK_BUCKET, original_file_key, f)
        
        try:
            if file_key.lower().endswith('.epub'):
                pages = extract_content_from_epub(tmp_file_path)
                print(f"Total pages in document: {len(pages)}")
                
                # Combine all content for embedding
                final_text = ""
                for page in pages:
                    final_text += page['html'] + "\n\n"
                
                # Create thumbnail from first page HTML
                if pages:
                    first_page = pages[0]
                    # Save first page HTML to a temporary file
                    temp_html = os.path.join(tmp_dir, "temp.html")
                    with open(temp_html, 'w') as f:
                        f.write(first_page['html'])
                    # Convert to PDF for thumbnail
                    temp_pdf = os.path.join(tmp_dir, "temp.pdf")
                    HTML(filename=temp_html).write_pdf(temp_pdf)
                    doc = fitz.open(temp_pdf)
                    os.remove(temp_html)
                    os.remove(temp_pdf)
            else:
                doc = fitz.open(tmp_file_path)
                final_text = ""
                print(f"Total pages in document: {len(doc)}")
                for page_number, page in enumerate(doc):
                    try:
                        text = page.get_text()
                        final_text += text
                        final_text += "\n\n"
                    except Exception as e:
                        print(f"Error processing page {page_number}: {e}")
                        continue
            
            # Perform text embedding
            text_embedding(file_key, final_text)
            
            # Create a new PDF file
            thumbfile = fitz.open()
            thumbfile.insert_pdf(doc, from_page=0, to_page=0)
            thumbfile.save(f"{file_key}")
            thumbfile.close()

            # Upload the pdf to s3
            with open(f"{file_key}", "rb") as pdf_file:
                upload_params = {
                    'Bucket': EBOOK_BUCKET,
                    'Key': f"{THUMBFILES_FOLDER}{file_key}",
                    'Body': pdf_file
                }
                S3_CLIENT.put_object(**upload_params)
                
            # Format URLs by replacing underscores with plus signs
            formatted_key = file_key.replace('_', '+')
            thumbfile_url = f"https://{EBOOK_BUCKET}.s3.{os.getenv('S3_REGION')}.amazonaws.com/{COMPRESSED_FOLDER}{formatted_key}"
            file_url = f"https://{EBOOK_BUCKET}.s3.{os.getenv('S3_REGION')}.amazonaws.com/{COMPRESSED_FOLDER}{formatted_key}"

            doc.close()

            # Delete the temporary PDF file
            os.remove(f"{file_key}")
        except Exception as e:
            print(f"Error processing file {file_key}: {e}")
            continue

        # Upload to S3 and save metadata to DynamoDB
        table.put_item(
            Item={
                'file_key': file_key,
                'title': file_key.replace('_', ' '),
                'upload_time': datetime.utcnow().isoformat(),
                'url': file_url,
                'thumb_url': thumbfile_url,
                'status': 'indexed'  # Changed status to 'indexed' since we've done the embedding
            }
        )
        print(f"Uploading file: {file_key} to S3...")
        os.remove(tmp_file_path)

    print("File processing completed.")

process_files()
