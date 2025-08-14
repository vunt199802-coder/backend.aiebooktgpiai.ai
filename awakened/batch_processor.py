import fitz
from openai import OpenAI, AsyncOpenAI
import pinecone
import boto3
import os
from typing import List, Dict, Optional
import logging
from datetime import datetime
import tempfile
import re
import asyncio
import aiohttp
import time
import uuid
import hashlib
import numpy as np
from botocore.exceptions import ClientError
import traceback
from dotenv import load_dotenv

class CustomLogger:
    def __init__(self, log_dir="logs"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.loggers = {}
        self.setup_loggers()

    def setup_loggers(self):
        logger_types = {
            'main': 'main_process.log',
            'file_ops': 'file_operations.log',
            'api_ops': 'api_operations.log',
            'db_ops': 'database_operations.log',
            'error': 'errors.log',
            'cost': 'cost_tracking.log',
            'progress': 'progress.log',
            'pinecone': 'pinecone_operations.log'  # Added Pinecone-specific logging
        }
        
        for name, filename in logger_types.items():
            self.setup_logger(name, filename)

    def setup_logger(self, name, filename):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # File handler
        fh = logging.FileHandler(
            os.path.join(self.log_dir, f"{self.timestamp}_{filename}")
        )
        fh.setLevel(logging.DEBUG)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)
        self.loggers[name] = logger

    def get_logger(self, name):
        return self.loggers.get(name)

class PineconeManager:
    def __init__(self, api_key: str, host: str, namespace: str):
        self.pc = pinecone.Pinecone(api_key=api_key)
        self.host = host
        self.namespace = namespace
        self.index = self.pc.Index(host=f"https://{host}")
        self.logger = logging.getLogger('pinecone')
        self.dimension = 1536  # text-embedding-3-small dimension

    async def verify_connection(self) -> bool:
        """Verify Pinecone connection and configuration"""
        try:
            # Get index stats
            stats = self.index.describe_index_stats()
            self.logger.info(
                f"Pinecone connection verified:\n"
                f"Total vectors: {stats.get('total_vector_count', 0)}\n"
                f"Dimension: {stats.get('dimension', 0)}\n"
                f"Namespaces: {list(stats.get('namespaces', {}).keys())}"
            )
            
            # Test vector operations
            test_id = f"test-{uuid.uuid4()}"
            test_vector = np.random.rand(self.dimension).tolist()
            
            # Test upsert
            self.index.upsert(
                vectors=[{
                    'id': test_id,
                    'values': test_vector,
                    'metadata': {'test': True}
                }],
                namespace=self.namespace
            )
            
            # Test query
            query_response = self.index.query(
                vector=test_vector,
                top_k=1,
                namespace=self.namespace,
                include_metadata=True
            )
            
            # Verify response
            if not query_response.matches or query_response.matches[0].id != test_id:
                raise Exception("Test vector query failed")
            
            # Cleanup test vector
            self.index.delete(ids=[test_id], namespace=self.namespace)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Pinecone verification failed: {str(e)}")
            return False

    async def batch_upsert(self, vectors: List[Dict], batch_size: int = 100) -> bool:
        """Upload vectors in batches with retry logic"""
        try:
            total_vectors = len(vectors)
            successful_uploads = 0
            
            for i in range(0, total_vectors, batch_size):
                batch = vectors[i:i + batch_size]
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries:
                    try:
                        self.index.upsert(
                            vectors=batch,
                            namespace=self.namespace
                        )
                        successful_uploads += len(batch)
                        self.logger.info(
                            f"Uploaded batch {i//batch_size + 1}/{(total_vectors + batch_size - 1)//batch_size} "
                            f"({successful_uploads}/{total_vectors} vectors)"
                        )
                        break
                    except Exception as e:
                        retry_count += 1
                        self.logger.error(f"Upload attempt {retry_count} failed: {str(e)}")
                        if retry_count == max_retries:
                            raise
                        await asyncio.sleep(2 ** retry_count)
            
            return successful_uploads == total_vectors
            
        except Exception as e:
            self.logger.error(f"Batch upsert failed: {str(e)}")
            return False

    def get_stats(self) -> Dict:
        """Get current index statistics"""
        try:
            stats = self.index.describe_index_stats()
            return {
                'total_vectors': stats.get('total_vector_count', 0),
                'dimension': stats.get('dimension', 0),
                'namespaces': stats.get('namespaces', {}),
                'index_fullness': stats.get('index_fullness', 0)
            }
        except Exception as e:
            self.logger.error(f"Failed to get stats: {str(e)}")
            return {}

class StorageProcessor:
    def __init__(self):
        load_dotenv()
        
        # Initialize loggers
        self.logger = logging.getLogger('file_ops')
        
        # Initialize S3
        self.bucket_name = os.getenv('S3_BUCKET')
        if not self.bucket_name:
            raise ValueError("S3_BUCKET environment variable is not set")

        self.s3_client = boto3.client(
            's3',
            region_name=os.getenv('S3_REGION', 'ap-southeast-2'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        # Initialize DynamoDB
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.getenv('S3_REGION', 'ap-southeast-2'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        self.table = dynamodb.Table(os.getenv('DYNAMODB_TABLE', 'file_processing_status'))
        self.indexed_prefix = os.getenv('S3_INDEXED_PREFIX', 'indexed/')
        self.completed_prefix = os.getenv('S3_COMPLETED_PREFIX', 'completed/')

    def _generate_file_id(self, file_key: str) -> str:
        """Generate consistent file ID from file key"""
        return hashlib.md5(file_key.encode('utf-8')).hexdigest()

    async def check_if_processed(self, file_key: str) -> Dict:
        """Check if a file has been processed"""
        file_id = self._generate_file_id(file_key)
        original_key = f"{self.indexed_prefix}{file_key}"
        completed_key = f"{self.completed_prefix}{file_key}"

        try:
            # Check S3 completed directory
            try:
                self.s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=completed_key
                )
                self.logger.info(f"File {file_key} found in completed directory")
                return {
                    "processed": True,
                    "location": "completed",
                    "file_id": file_id
                }
            except ClientError:
                pass

            # Check DynamoDB
            response = self.table.get_item(
                Key={'file_id': file_id}
            )

            if 'Item' in response and response['Item'].get('embedding_status') == 'completed':
                self.logger.info(f"File {file_key} marked as completed in DynamoDB")
                return {
                    "processed": True,
                    "location": "dynamodb",
                    "file_id": file_id
                }

            return {
                "processed": False,
                "file_id": file_id,
                "original_key": original_key,
                "completed_key": completed_key
            }

        except Exception as e:
            self.logger.error(f"Error checking processing status: {str(e)}")
            raise

    async def move_to_completed(self, file_info: Dict) -> bool:
        """Move processed file to completed directory"""
        try:
            # Copy to completed directory
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={
                    'Bucket': self.bucket_name,
                    'Key': file_info['original_key']
                },
                Key=file_info['completed_key']
            )

            # Update DynamoDB status
            self.table.update_item(
                Key={'file_id': file_info['file_id']},
                UpdateExpression="SET storage_status = :status, completed_time = :time",
                ExpressionAttributeValues={
                    ':status': 'completed',
                    ':time': datetime.utcnow().isoformat()
                }
            )

            self.logger.info(f"File {file_info['file_id']} moved to completed status")
            return True

        except Exception as e:
            self.logger.error(f"Error moving file to completed: {str(e)}")
            return False

    async def list_pending_files(self) -> List[Dict]:
        """List all pending PDF files"""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pending_files = []

            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=self.indexed_prefix
            ):
                for obj in page.get('Contents', []):
                    file_key = obj['Key'].replace(self.indexed_prefix, '')

                    if not file_key or not file_key.lower().endswith('.pdf'):
                        continue

                    status = await self.check_if_processed(file_key)
                    if not status['processed']:
                        pending_files.append({
                            'file_key': file_key,
                            'file_id': status['file_id'],
                            'original_key': status['original_key'],
                            'completed_key': status['completed_key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })

            self.logger.info(f"Found {len(pending_files)} pending files")
            return pending_files

        except Exception as e:
            self.logger.error(f"Error listing pending files: {str(e)}")
            raise

    async def download_pdf(self, file_info: Dict, local_path: str):
        """Download PDF file to local path"""
        try:
            start_time = time.time()
            
            self.s3_client.download_file(
                Bucket=self.bucket_name,
                Key=file_info['original_key'],
                Filename=local_path
            )

            file_size = os.path.getsize(local_path)
            download_time = time.time() - start_time
            
            self.logger.info(
                f"Downloaded {file_info['file_key']} "
                f"({file_size/1024/1024:.2f} MB) in {download_time:.2f}s"
            )

        except Exception as e:
            self.logger.error(f"Error downloading PDF: {str(e)}")
            raise

    async def update_processing_status(self, file_id: str, status: str, metadata: Dict = None):
        """Update processing status in DynamoDB"""
        try:
            update_expr = "SET processing_status = :status, last_updated = :time"
            expr_values = {
                ':status': status,
                ':time': datetime.utcnow().isoformat()
            }

            if metadata:
                update_expr += ", metadata = :metadata"
                expr_values[':metadata'] = metadata

            self.table.update_item(
                Key={'file_id': file_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_values
            )

            self.logger.info(f"Updated status for {file_id} to {status}")

        except Exception as e:
            self.logger.error(f"Error updating processing status: {str(e)}")
            raise

class BatchPDFProcessor:
    def __init__(self):
        load_dotenv()

        # Initialize loggers
        self.custom_logger = CustomLogger()
        self.main_logger = self.custom_logger.get_logger('main')
        self.file_logger = self.custom_logger.get_logger('file_ops')
        self.api_logger = self.custom_logger.get_logger('api_ops')
        self.db_logger = self.custom_logger.get_logger('db_ops')
        self.error_logger = self.custom_logger.get_logger('error')
        self.cost_logger = self.custom_logger.get_logger('cost')
        self.progress_logger = self.custom_logger.get_logger('progress')
        
        self.main_logger.info("Initializing BatchPDFProcessor")

        # Initialize OpenAI client
        self.openai_client = AsyncOpenAI(
            api_key=os.getenv("OPENAI_API_KEY")
        )

        # Initialize Pinecone
        self.pinecone_manager = PineconeManager(
            api_key=os.getenv("PINECONE_API_KEY"),
            host="ebooks-store-zaudryq.svc.aped-4627-b74a.pinecone.io",
            namespace="ebooks-store-b7a7f3f3"
        )

        # Initialize storage processor
        self.storage_processor = StorageProcessor()

        # Processing constants
        self.PDF_BATCH_SIZE = 5
        self.EMBEDDING_BATCH_SIZE = 50
        self.CHUNK_SIZE = 1000
        self.CHUNK_OVERLAP = 200

        # Cost tracking
        self.cost_tracking = {
            'embedding_tokens': 0,
            'gpt4_tokens': 0,
            'api_calls': {
                'embedding': 0,
                'gpt4': 0,
                'pinecone': 0
            }
        }

    async def verify_pinecone(self) -> bool:
        """Verify Pinecone connection and basic operations"""
        try:
            # Get current stats
            stats = self.pinecone_manager.get_stats()
            self.main_logger.info(
                f"Connected to Pinecone index:\n"
                f"Total vectors: {stats.get('total_vectors', 0)}\n"
                f"Dimension: {stats.get('dimension', 1536)}\n"
                f"Namespaces: {list(stats.get('namespaces', {}).keys())}"
            )

            # Test vector operations
            test_vector = np.random.rand(1536).tolist()
            test_id = f"test-{uuid.uuid4()}"

            # Test upsert
            vectors = [{
                'id': test_id,
                'values': test_vector,
                'metadata': {'test': True}
            }]

            await self.pinecone_manager.batch_upsert(vectors)
            self.main_logger.info("Test vector upload successful")

            # Test query
            response = self.pinecone_manager.index.query(
                vector=test_vector,
                top_k=1,
                namespace=self.pinecone_manager.namespace,
                include_metadata=True
            )
            
            if response.matches and response.matches[0].id == test_id:
                self.main_logger.info("Test vector query successful")
            else:
                raise Exception("Test vector query failed to return expected result")

            # Cleanup test vector
            self.pinecone_manager.index.delete(
                ids=[test_id], 
                namespace=self.pinecone_manager.namespace
            )
            self.main_logger.info("Test vector cleanup successful")

            return True

        except Exception as e:
            self.error_logger.error(
                f"Pinecone verification failed:\n"
                f"Error type: {type(e).__name__}\n"
                f"Error message: {str(e)}"
            )
            return False



    async def process_pdf_batch(self, batch: List[Dict]) -> List[Dict]:
        """Process a batch of PDFs"""
        results = []
        batch_id = uuid.uuid4().hex[:8]
        
        for file_info in batch:
            try:
                self.main_logger.info(f"Processing PDF {file_info['file_id']}")
                
                async with aiohttp.ClientSession() as session:
                    result = await self.process_single_pdf(session, file_info, batch_id)
                    
                if result and isinstance(result, dict):
                    if result.get('status') == 'success':
                        self.main_logger.info(
                            f"Successfully processed {file_info['file_id']}\n"
                            f"Vectors: {result.get('vectors_uploaded', 0)}\n"
                            f"Time: {result.get('processing_time', 0):.2f}s"
                        )
                    else:
                        self.error_logger.error(
                            f"Failed to process {file_info['file_id']}: {result.get('error')}"
                        )
                else:
                    result = {
                        'file_id': file_info['file_id'],
                        'status': 'error',
                        'error': 'Invalid result format'
                    }
                    
                results.append(result)
                
            except Exception as e:
                self.error_logger.error(
                    f"Error processing {file_info['file_id']}: {str(e)}\n"
                    f"Stack trace: {traceback.format_exc()}"
                )
                results.append({
                    'file_id': file_info['file_id'],
                    'status': 'error',
                    'error': str(e)
                })

        return results

    async def process_single_pdf(self, session, pdf_info: Dict, batch_id: str) -> Dict:
        """Process a single PDF file"""
        pdf_id = pdf_info['file_id']
        start_time = time.time()

        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                # Download and process PDF
                tmp_path = os.path.join(tmp_dir, f"{pdf_id}.pdf")
                await self.storage_processor.download_pdf(pdf_info, tmp_path)

                doc = fitz.open(tmp_path)
                total_pages = len(doc)
                all_chunks = []
                processed_pages = 0

                # Process each page
                for page_num in range(total_pages):
                    self.main_logger.info(f"Processing page {page_num + 1}/{total_pages}")
                    
                    page = doc[page_num]
                    page_data = await self.process_page(page, page_num, pdf_id)
                    chunks = self.create_chunks(page_data)
                    all_chunks.extend(chunks)
                    processed_pages += 1

                doc.close()

                # Create embeddings and upload
                embeddings = await self.batch_create_embeddings(all_chunks, pdf_id)
                upload_success = await self.batch_upload_vectors(pdf_id, embeddings, all_chunks)

                if upload_success:
                    # Move to completed
                    await self.storage_processor.move_to_completed(pdf_info)
                    
                    processing_time = time.time() - start_time
                    return {
                        'file_id': pdf_id,
                        'status': 'success',
                        'pages_processed': processed_pages,
                        'chunks_created': len(all_chunks),
                        'vectors_uploaded': len(embeddings),
                        'processing_time': processing_time
                    }
                else:
                    raise Exception("Failed to upload vectors to Pinecone")

        except Exception as e:
            self.error_logger.error(f"Error processing PDF {pdf_id}: {str(e)}")
            return {
                'file_id': pdf_id,
                'status': 'error',
                'error': str(e)
            }

    async def process_page(self, page: fitz.Page, page_num: int, pdf_id: str) -> Dict:
        """Process a single page"""
        try:
            text = page.get_text("text")
            layout_description = await self._get_layout_description(text, page_num, page.parent.page_count)

            return {
                'page_num': page_num + 1,
                'text': text,
                'layout_description': layout_description
            }

        except Exception as e:
            self.error_logger.error(f"Error processing page {page_num + 1}: {str(e)}")
            raise

    async def _get_layout_description(self, text: str, page_num: int, total_pages: int) -> str:
        """Get layout description using GPT-4o-mini"""
        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "Analyze and describe document layouts concisely in English and Malay."
                    },
                    {
                        "role": "user",
                        "content": self._generate_layout_prompt(text, page_num, total_pages)
                    }
                ],
                max_tokens=150,
                temperature=0.3
            )

            self.cost_tracking['gpt4_tokens'] += response.usage.total_tokens
            self.cost_tracking['api_calls']['gpt4'] += 1

            return response.choices[0].message.content

        except Exception as e:
            self.error_logger.error(f"Error getting layout description: {str(e)}")
            return ""

    def _generate_layout_prompt(self, text: str, page_num: int, total_pages: int) -> str:
        """Generate layout analysis prompt"""
        return f"""Analyze page {page_num + 1}/{total_pages}:

Text content:
{text}

Identify and describe:
1. Main content type (text, dialogue, images)
2. Headers/titles
3. Layout structure
4. Key content positions

Provide analysis in clear, structured format."""

    def create_chunks(self, page_data: Dict) -> List[Dict]:
        """Create chunks from page text"""
        try:
            chunks = []
            text = page_data['text']
            page_num = page_data['page_num']

            # Split into sentences
            sentences = re.split(r'(?<=[.!?])\s+', text)
            current_chunk = []
            current_length = 0

            for sentence in sentences:
                sentence_length = len(sentence)

                if current_length + sentence_length > self.CHUNK_SIZE:
                    if current_chunk:
                        chunks.append({
                            'text': ' '.join(current_chunk),
                            'layout_description': page_data['layout_description'],
                            'page_numbers': [str(page_num)]
                        })
                    current_chunk = [sentence]
                    current_length = sentence_length
                else:
                    current_chunk.append(sentence)
                    current_length += sentence_length

            # Add final chunk
            if current_chunk:
                chunks.append({
                    'text': ' '.join(current_chunk),
                    'layout_description': page_data['layout_description'],
                    'page_numbers': [str(page_num)]
                })

            return chunks

        except Exception as e:
            self.error_logger.error(f"Error creating chunks: {str(e)}")
            return []
    async def batch_create_embeddings(self, chunks: List[Dict], pdf_id: str) -> List[List[float]]:
        """Create embeddings for chunks in batches"""
        embeddings = []
        start_time = time.time()

        for i in range(0, len(chunks), self.EMBEDDING_BATCH_SIZE):
            batch = chunks[i:i + self.EMBEDDING_BATCH_SIZE]
            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    texts = [
                        f"Text: {chunk['text']}\nLayout: {chunk['layout_description']}"
                        for chunk in batch
                    ]

                    response = await self.openai_client.embeddings.create(
                        model="text-embedding-3-small",
                        input=texts,
                        encoding_format="float"
                    )

                    total_tokens = sum(len(text.split()) for text in texts)
                    self.cost_tracking['embedding_tokens'] += total_tokens
                    self.cost_tracking['api_calls']['embedding'] += 1

                    batch_embeddings = [data.embedding for data in response.data]
                    embeddings.extend(batch_embeddings)

                    self.api_logger.info(
                        f"Created embeddings batch {i//self.EMBEDDING_BATCH_SIZE + 1}: "
                        f"{len(batch)} chunks, {total_tokens} tokens"
                    )
                    break

                except Exception as e:
                    retry_count += 1
                    self.error_logger.error(f"Embedding creation attempt {retry_count} failed: {str(e)}")
                    if retry_count == max_retries:
                        raise
                    await asyncio.sleep(2 ** retry_count)

        self.main_logger.info(
            f"Created {len(embeddings)} embeddings in {time.time() - start_time:.2f}s"
        )
        return embeddings

    async def batch_upload_vectors(self, file_id: str, embeddings: List[List[float]], chunks: List[Dict]) -> bool:
        """Upload vectors to Pinecone"""
        vectors = []
        for idx, (embedding, chunk) in enumerate(zip(embeddings, chunks)):
            if not embedding or len(embedding) != 1536:
                self.error_logger.error(
                    f"Invalid embedding for chunk {idx}: "
                    f"Expected dim 1536, got {len(embedding) if embedding else 'None'}"
                )
                continue

            # Clean and truncate metadata
            metadata = {
                'text': str(chunk.get('text', ''))[:3000],
                'layout_description': str(chunk.get('layout_description', ''))[:1000],
                'page_number': str(chunk.get('page_numbers', [''])[0]),
                'file_id': str(file_id),
                'chunk_id': str(idx),
                'timestamp': datetime.utcnow().isoformat()
            }

            vectors.append({
                'id': f"{file_id}-{idx}",
                'values': embedding,
                'metadata': metadata
            })

        return await self.pinecone_manager.batch_upsert(vectors)

    def estimate_costs(self) -> Dict:
        """Calculate cost estimates"""
        GPT4O_MINI_COST_PER_1K_TOKENS = 0.0075
        EMBEDDING_COST_PER_1K_TOKENS = 0.00002

        gpt_cost = (self.cost_tracking['gpt4_tokens'] / 1000) * GPT4O_MINI_COST_PER_1K_TOKENS
        embedding_cost = (self.cost_tracking['embedding_tokens'] / 1000) * EMBEDDING_COST_PER_1K_TOKENS

        cost_report = {
            'gpt4_tokens': self.cost_tracking['gpt4_tokens'],
            'embedding_tokens': self.cost_tracking['embedding_tokens'],
            'api_calls': self.cost_tracking['api_calls'],
            'gpt_cost': gpt_cost,
            'embedding_cost': embedding_cost,
            'total_cost': gpt_cost + embedding_cost
        }

        self.cost_logger.info(
            f"\nCost Report:\n"
            f"GPT4 Tokens: {cost_report['gpt4_tokens']}\n"
            f"Embedding Tokens: {cost_report['embedding_tokens']}\n"
            f"API Calls:\n"
            f"  - GPT4: {cost_report['api_calls']['gpt4']}\n"
            f"  - Embedding: {cost_report['api_calls']['embedding']}\n"
            f"  - Pinecone: {cost_report['api_calls']['pinecone']}\n"
            f"Costs:\n"
            f"  - GPT4: ${cost_report['gpt_cost']:.4f}\n"
            f"  - Embedding: ${cost_report['embedding_cost']:.4f}\n"
            f"  - Total: ${cost_report['total_cost']:.4f}"
        )

        return cost_report

    async def process_all_pdfs(self):
        """Process a single PDF file for testing"""
        start_time = time.time()
        
        try:
            # Get current stats
            stats = self.pinecone_manager.get_stats()
            self.main_logger.info(
                f"\nInitial Pinecone Stats:\n"
                f"Total vectors: {stats.get('total_vectors', 0)}\n"
                f"Namespaces: {list(stats.get('namespaces', {}).keys())}"
            )

            # Get pending files
            pending_files = await self.storage_processor.list_pending_files()
            
            if not pending_files:
                self.main_logger.info("No pending files found to process")
                return

            # Process single file
            test_file = pending_files[0]
            self.main_logger.info(
                f"\nProcessing test file:\n"
                f"File: {test_file['file_key']}\n"
                f"ID: {test_file['file_id']}\n"
                f"Size: {test_file['size']/1024/1024:.2f} MB"
            )

            # Process file
            async with aiohttp.ClientSession() as session:
                result = await self.process_single_pdf(
                    session, 
                    test_file,
                    "test-run"
                )
            
            if result and result.get('status') == 'success':
                self.main_logger.info(
                    f"\nProcessing successful:\n"
                    f"Pages: {result.get('pages_processed', 0)}\n"
                    f"Chunks: {result.get('chunks_created', 0)}\n"
                    f"Vectors: {result.get('vectors_uploaded', 0)}\n"
                    f"Time: {result.get('processing_time', 0):.2f}s"
                )
                
                await self.storage_processor.update_processing_status(
                    test_file['file_id'],
                    'completed',
                    {'vectors': result.get('vectors_uploaded', 0)}
                )
            else:
                error_msg = result.get('error') if result else 'Unknown error'
                self.error_logger.error(f"Processing failed: {error_msg}")
                
                await self.storage_processor.update_processing_status(
                    test_file['file_id'],
                    'failed',
                    {'error': error_msg}
                )

            # Get final stats
            final_stats = self.pinecone_manager.get_stats()
            vectors_added = final_stats.get('total_vectors', 0) - stats.get('total_vectors', 0)
            
            self.main_logger.info(
                f"\nFinal Stats:\n"
                f"Total vectors: {final_stats.get('total_vectors', 0)}\n"
                f"Vectors added: {vectors_added}"
            )

            # Show costs
            costs = self.estimate_costs()
            self.cost_logger.info(
                f"\nTest Run Costs:\n"
                f"GPT4 tokens: {costs['gpt4_tokens']:,}\n"
                f"Embedding tokens: {costs['embedding_tokens']:,}\n"
                f"Total cost: ${costs['total_cost']:.4f}"
            )

        except Exception as e:
            self.error_logger.error(
                f"Error in test run:\n"
                f"Error type: {type(e).__name__}\n"
                f"Error message: {str(e)}\n"
                f"Stack trace: {traceback.format_exc()}"
            )
            raise


async def main():
    """Main entry point with better error handling"""
    processor = None
    try:
        # Initialize processor
        processor = BatchPDFProcessor()
        
        # Test Pinecone connection
        if await processor.verify_pinecone():
            processor.main_logger.info("Pinecone connection verified, starting processing...")
            await processor.process_all_pdfs()
        else:
            processor.main_logger.error("Failed to verify Pinecone connection, aborting process")
            
    except Exception as e:
        if processor:
            processor.error_logger.error(
                f"Fatal error in processing:\n"
                f"Error type: {type(e).__name__}\n"
                f"Error message: {str(e)}\n"
                f"Stack trace: {traceback.format_exc()}"
            )
        else:
            logging.error(f"Failed to initialize processor: {str(e)}")
        raise
    finally:
        if processor:
            processor.main_logger.info("Processing completed")

if __name__ == "__main__":
    asyncio.run(main())