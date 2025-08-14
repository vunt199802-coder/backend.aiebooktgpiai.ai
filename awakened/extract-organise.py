import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
import zipfile
import shutil
from pathlib import Path
import logging
from tqdm import tqdm
import unicodedata
import tempfile
import hashlib
import re
import time
import filetype
from botocore.exceptions import ClientError

# Remove any existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ebook_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("Starting the EbookProcessor application")

# Set a custom temporary directory
os.environ['TMPDIR'] = '/tmp'

class EbookProcessor:
    def __init__(self):
        load_dotenv(".env")
        
        # Initialize AWS clients
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("S3_REGION")
        )
        
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.getenv("DYNAMODB_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        
        # Set up constants
        self.ORIGINAL_BUCKET = "primary-school-ebook-data"
        self.INDEXED_BUCKET = "primary-school-ebook-data"
        self.UPLOAD_PREFIX = "uploads/"
        self.INDEXED_PREFIX = "indexed/"
        self.table = self.dynamodb.Table('ebooks')
        self.processed_zips_table = self.dynamodb.Table('ProcessedZips')

    def normalize_filename(self, filename: str) -> str:
        """Normalize filename while preserving Unicode characters."""
        # Normalize Unicode (NFKC form combines compatibility characters)
        normalized = unicodedata.normalize('NFKC', filename)
        
        # Replace problematic characters but keep Unicode letters and numbers
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', normalized)
        
        # Ensure the filename isn't too long
        if len(safe_filename.encode('utf-8')) > 255:
            name_hash = hashlib.md5(filename.encode('utf-8')).hexdigest()[:8]
            extension = Path(safe_filename).suffix
            safe_filename = f"{name_hash}{extension}"
            
        return safe_filename

    def get_normalized_key(self, zip_path: str) -> str:
        # Extract the file name as a consistent identifier
        return os.path.basename(zip_path)

    def is_zip_processed(self, zip_path: str) -> bool:
        normalized_key = self.get_normalized_key(zip_path)
        try:
            logger.info(f"Checking if ZIP is processed: {normalized_key}")
            response = self.processed_zips_table.get_item(Key={'zip_path': normalized_key})
            is_processed = 'Item' in response
            logger.info(f"ZIP processed status for {normalized_key}: {is_processed}")
            return is_processed
        except ClientError as e:
            logger.error(f"Error checking if ZIP is processed: {e}")
            return False

    def mark_zip_as_processed(self, zip_path: str):
        normalized_key = self.get_normalized_key(zip_path)
        try:
            logger.info(f"Marking ZIP as processed: {normalized_key}")
            self.processed_zips_table.put_item(Item={'zip_path': normalized_key})
        except ClientError as e:
            logger.error(f"Error marking ZIP as processed: {e}")

    def process_zip_file(self, zip_path: str, tmp_dir: str) -> list:
        """Extract and process ZIP file with better error handling."""
        extracted_files = []
        
        # Check if the ZIP file has already been processed
        if self.is_zip_processed(zip_path):
            logger.info(f"ZIP file {zip_path} already processed, skipping extraction.")
            return extracted_files
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List all files before extraction
                for file_info in zip_ref.filelist:
                    try:
                        # Skip directories
                        if file_info.filename.endswith('/'):
                            continue
                            
                        # Check if it's a supported file type
                        if not any(file_info.filename.lower().endswith(ext) 
                                 for ext in ['.pdf', '.epub', '.mobi']):
                            continue
                            
                        # Normalize the filename
                        original_name = file_info.filename
                        if not isinstance(original_name, str):
                            original_name = original_name.decode('utf-8', errors='replace')
                            
                        safe_name = self.normalize_filename(original_name)
                        extract_path = os.path.join(tmp_dir, safe_name)
                        
                        # Create directory if it doesn't exist
                        os.makedirs(os.path.dirname(extract_path), exist_ok=True)
                        
                        # Extract the file
                        with zip_ref.open(file_info) as source, \
                             open(extract_path, 'wb') as target:
                            shutil.copyfileobj(source, target)
                            
                        extracted_files.append((extract_path, original_name))
                        logger.info(f"Extracted: {original_name}")
                        
                    except Exception as e:
                        logger.error(f"Error extracting {file_info.filename}: {e}")
                        continue
                        
            # Mark the ZIP file as processed
            self.mark_zip_as_processed(zip_path)

        except Exception as e:
            logger.error(f"Error processing ZIP file {zip_path}: {e}")
            
        return extracted_files

    def process_file(self, file_path: str, original_filename: str) -> bool:
        """Process a single file and upload to indexed folder."""
        try:
            # Use the custom temporary directory
            with tempfile.TemporaryDirectory(dir=os.environ['TMPDIR']) as tmp_dir:
                # Generate a unique file ID
                file_id = hashlib.md5(original_filename.encode('utf-8')).hexdigest()
                
                # Check if already processed
                try:
                    response = self.table.get_item(Key={'file_key': original_filename})
                    if 'Item' in response:
                        logger.info(f"File {original_filename} already processed, skipping...")
                        return False
                except Exception as e:
                    logger.error(f"Error checking DynamoDB: {e}")
                    return False
                
                # Upload file to indexed folder
                safe_filename = self.normalize_filename(original_filename)
                s3_key = f"{self.INDEXED_PREFIX}{safe_filename}"
                
                # Determine content type using filetype library
                kind = filetype.guess(file_path)
                if kind is None:
                    logger.error(f"Cannot determine file type for {original_filename}")
                    return False

                content_type = kind.mime

                # Upload to S3
                logger.info(f"Uploading {original_filename} to indexed folder...")
                self.s3_client.upload_file(
                    file_path,
                    self.INDEXED_BUCKET,
                    s3_key,
                    ExtraArgs={'ContentType': content_type}
                )
                
                # Update DynamoDB
                self.table.put_item(
                    Item={
                        'file_key': original_filename,
                        'file_id': file_id,
                        'normalized_name': safe_filename,
                        'upload_time': datetime.utcnow().isoformat(),
                        'status': 'uploaded',  # Changed from 'indexed' since we're not embedding
                        'url': f"https://{self.INDEXED_BUCKET}.s3.{os.getenv('S3_REGION')}.amazonaws.com/{s3_key}"
                    }
                )
                
                logger.info(f"Successfully processed {original_filename}")
                return True
                
        except Exception as e:
            logger.error(f"Error processing {original_filename}: {e}")
            return False

    def process_all_files(self):
        """Main processing function with progress tracking."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                # List all files in uploads directory
                response = self.s3_client.list_objects_v2(
                    Bucket=self.ORIGINAL_BUCKET,
                    Prefix=self.UPLOAD_PREFIX
                )
                
                # List all files in indexed directory
                indexed_response = self.s3_client.list_objects_v2(
                    Bucket=self.INDEXED_BUCKET,
                    Prefix=self.INDEXED_PREFIX
                )
                indexed_files = {obj['Key'] for obj in indexed_response.get('Contents', [])}

                total_files = len(response.get('Contents', []))
                processed_files = 0
                failed_files = 0
                batch_size = 10  # Process 10 files at a time
                start_time = time.time()
                
                with tqdm(total=total_files, desc="Processing files") as pbar:
                    for i in range(0, total_files, batch_size):
                        batch = response.get('Contents', [])[i:i + batch_size]
                        
                        for obj in batch:
                            file_key = obj['Key']
                            if file_key == self.UPLOAD_PREFIX:
                                continue
                            
                            # Check if file is already indexed
                            if f"{self.INDEXED_PREFIX}{Path(file_key).name}" in indexed_files:
                                logger.info(f"File {file_key} already indexed, skipping...")
                                continue
                            
                            original_filename = Path(file_key).name
                            download_path = os.path.join(tmp_dir, self.normalize_filename(original_filename))
                                   
                            if self.is_zip_processed(download_path):
                                logger.info(f"ZIP file {download_path} already processed, skipping...")
                                continue
                            try:
                                # Download file
                                logger.info(f"Downloading {original_filename}...")
                                self.s3_client.download_file(
                                    self.ORIGINAL_BUCKET,
                                    file_key,
                                    download_path
                                )
                                
                                # Process based on file type
                                if download_path.lower().endswith('.zip'):
                                    extracted_files = self.process_zip_file(download_path, tmp_dir)
                                    for extracted_path, original_name in extracted_files:
                                        success = self.process_file(
                                            extracted_path,
                                            original_name
                                        )
                                        if success:
                                            processed_files += 1
                                        else:
                                            failed_files += 1
                                        # Ensure temporary files are cleaned up after processing
                                        os.remove(extracted_path)
                                else:
                                    success = self.process_file(
                                        download_path,
                                        original_filename
                                    )
                                    if success:
                                        processed_files += 1
                                    else:
                                        failed_files += 1
                                    # Ensure temporary files are cleaned up after processing
                                    os.remove(download_path)
                            except Exception as e:
                                logger.error(f"Error processing {original_filename}: {e}")
                                failed_files += 1
                                
                            # Log remaining files and estimated time
                            remaining_files = total_files - processed_files - failed_files
                            elapsed_time = time.time() - start_time
                            avg_time_per_file = elapsed_time / (processed_files + failed_files)
                            estimated_time_left = avg_time_per_file * remaining_files
                            logger.info(f"Remaining files: {remaining_files}, Estimated time left: {estimated_time_left:.2f} seconds")
                            
                            pbar.update(1)
                
                logger.info(f"""
                Processing completed:
                - Total files: {total_files}
                - Successfully processed: {processed_files}
                - Failed: {failed_files}
                """)
                
            except Exception as e:
                logger.error(f"Error processing files: {e}")

def main():
    processor = EbookProcessor()
    processor.process_all_files()

if __name__ == "__main__":
    main()