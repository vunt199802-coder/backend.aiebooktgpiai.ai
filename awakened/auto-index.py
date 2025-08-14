import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
import zipfile
import shutil
from pathlib import Path
import logging
from tqdm import tqdm
import fitz
import unicodedata
from typing import List, Dict, Any, Tuple
import tempfile
import hashlib
from openai import OpenAI
from pinecone import Pinecone  # Updated import
import time
from concurrent.futures import ThreadPoolExecutor
import json
import re

# Set up logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ebook_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
        
        # Initialize OpenAI
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        # Initialize Pinecone with new method
        self.pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        self.pinecone_index = self.pc.Index(os.getenv("PINECONE_INDEX"))
        self.pinecone_namespace = os.getenv("PINECONE_NAMESPACE")
        
        # Set up constants
        self.ORIGINAL_BUCKET = "primary-school-ebook-data"
        self.INDEXED_BUCKET = "primary-school-ebook-data"
        self.UPLOAD_PREFIX = "uploads/"
        self.INDEXED_PREFIX = "indexed/"
        self.table = self.dynamodb.Table('ebooks')
        
        # Chunk sizes and limits
        self.MAX_CHUNK_SIZE = 1000
        self.MAX_VECTORS_PER_BATCH = 100

    def normalize_filename(self, filename: str) -> str:
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

    def extract_text_from_pdf(self, file_path: str) -> str:
        """Extract text from PDF with progress bar and error handling."""
        try:
            doc = fitz.open(file_path)
            text_parts = []
            
            for page_num in tqdm(range(len(doc)), desc=f"Extracting text from {Path(file_path).name}"):
                try:
                    page = doc[page_num]
                    text = page.get_text()
                    text_parts.append(text)
                except Exception as e:
                    logger.warning(f"Error extracting text from page {page_num}: {e}")
                    continue
                    
            doc.close()
            return "\n".join(text_parts)
        except Exception as e:
            logger.error(f"Error processing PDF {file_path}: {e}")
            return ""

    def create_embeddings(self, text: str, file_id: str) -> List[Dict]:
        """Create embeddings for text chunks."""
        chunks = self.chunk_text(text)
        embeddings = []
        
        for i, chunk in enumerate(tqdm(chunks, desc="Creating embeddings")):
            try:
                response = self.openai_client.embeddings.create(
                    model="text-embedding-ada-002",
                    input=chunk
                )
                
                embedding = {
                    'id': f"{file_id}_chunk_{i}",
                    'values': response.data[0].embedding,
                    'metadata': {
                        'file_id': file_id,
                        'chunk_index': i,
                        'total_chunks': len(chunks),
                        'text': chunk[:1000]  # Store first 1000 chars of text
                    }
                }
                embeddings.append(embedding)
                
            except Exception as e:
                logger.error(f"Error creating embedding for chunk {i}: {e}")
                continue
                
        return embeddings

    def upload_embeddings(self, embeddings: List[Dict]):
        """Upload embeddings to Pinecone in batches."""
        for i in range(0, len(embeddings), self.MAX_VECTORS_PER_BATCH):
            batch = embeddings[i:i + self.MAX_VECTORS_PER_BATCH]
            
            try:
                self.pinecone_index.upsert(
                    vectors=batch,
                    namespace=self.pinecone_namespace
                )
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Error uploading embeddings batch: {e}")

    def process_zip_file(self, zip_path: str, tmp_dir: str) -> List[str]:
        """Extract and process ZIP file with better error handling."""
        extracted_files = []
        
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
                        
                        # Extract the file
                        with zip_ref.open(file_info) as source, \
                             open(extract_path, 'wb') as target:
                            shutil.copyfileobj(source, target)
                            
                        extracted_files.append(extract_path)
                        
                    except Exception as e:
                        logger.error(f"Error extracting {file_info.filename}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error processing ZIP file {zip_path}: {e}")
            
        return extracted_files

    def chunk_text(self, text: str) -> List[str]:
        """Split text into chunks of appropriate size."""
        chunks = []
        current_chunk = []
        current_size = 0
        
        for sentence in text.split('.'):
            sentence = sentence.strip() + '.'
            sentence_size = len(sentence.encode('utf-8'))
            
            if current_size + sentence_size > self.MAX_CHUNK_SIZE:
                if current_chunk:
                    chunks.append(' '.join(current_chunk))
                current_chunk = [sentence]
                current_size = sentence_size
            else:
                current_chunk.append(sentence)
                current_size += sentence_size
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
            
        return chunks

    def process_file(self, file_path: str, original_filename: str) -> bool:
        """Process a single file with better error handling."""
        try:
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
            
            # Extract text
            text = self.extract_text_from_pdf(file_path)
            if not text:
                logger.error(f"No text extracted from {original_filename}")
                return False
            
            # Create embeddings
            embeddings = self.create_embeddings(text, file_id)
            if not embeddings:
                logger.error(f"No embeddings created for {original_filename}")
                return False
            
            # Upload embeddings
            self.upload_embeddings(embeddings)
            
            # Upload file to indexed folder
            safe_filename = self.normalize_filename(original_filename)
            s3_key = f"{self.INDEXED_PREFIX}{safe_filename}"
            
            self.s3_client.upload_file(
                file_path,
                self.INDEXED_BUCKET,
                s3_key,
                ExtraArgs={'ContentType': 'application/pdf'}
            )
            
            # Update DynamoDB
            self.table.put_item(
                Item={
                    'file_key': original_filename,
                    'file_id': file_id,
                    'normalized_name': safe_filename,
                    'upload_time': datetime.utcnow().isoformat(),
                    'status': 'indexed',
                    'total_chunks': len(embeddings),
                    'url': f"https://{self.INDEXED_BUCKET}.s3.{os.getenv('S3_REGION')}.amazonaws.com/{s3_key}"
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing {original_filename}: {e}")
            return False

    def process_all_files(self):
        """Main processing function with better progress tracking."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                # List all files in uploads directory
                response = self.s3_client.list_objects_v2(
                    Bucket=self.ORIGINAL_BUCKET,
                    Prefix=self.UPLOAD_PREFIX
                )
                
                total_files = len(response.get('Contents', []))
                processed_files = 0
                failed_files = 0
                
                with tqdm(total=total_files, desc="Processing files") as pbar:
                    for obj in response.get('Contents', []):
                        file_key = obj['Key']
                        if file_key == self.UPLOAD_PREFIX:
                            continue
                            
                        original_filename = Path(file_key).name
                        download_path = os.path.join(tmp_dir, self.normalize_filename(original_filename))
                        
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
                            for extracted_file in extracted_files:
                                success = self.process_file(
                                    extracted_file,
                                    Path(extracted_file).name
                                )
                                if success:
                                    processed_files += 1
                                else:
                                    failed_files += 1
                        else:
                            success = self.process_file(
                                download_path,
                                original_filename
                            )
                            if success:
                                processed_files += 1
                            else:
                                failed_files += 1
                        
                        pbar.update(1)
                
                logger.info(f"""
                Processing completed:
                - Total files: {total_files}
                - Successfully processed: {processed_files}
                - Failed: {failed_files}
                """)
                
            except Exception as e:
                logger.error(f"Error in process_all_files: {e}")
            finally:
                # Clean up temp directory
                shutil.rmtree(tmp_dir, ignore_errors=True)

def main():
    processor = EbookProcessor()
    processor.process_all_files()

if __name__ == "__main__":
    main()