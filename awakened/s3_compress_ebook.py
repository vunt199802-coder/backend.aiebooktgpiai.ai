import os
import boto3
import tempfile
import subprocess
import logging
from datetime import datetime
import time
from humanize import naturalsize
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from threading import Lock
from typing import Optional, Tuple, Dict, List
from dotenv import load_dotenv

class PDFCompressor:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region: str,
                 bucket: str, indexed_prefix: str, compressed_prefix: str, max_workers: int = 4):
        """Initialize the PDF compressor with AWS credentials and S3 configuration."""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        self.bucket = bucket
        self.indexed_prefix = indexed_prefix
        self.compressed_prefix = compressed_prefix
        self.max_workers = max_workers
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Thread-safe counters
        self.stats_lock = Lock()
        self.total_original_size = 0
        self.total_compressed_size = 0
        self.start_time = None
        
        # Cache for processed files
        self.processed_files_cache = set()

    def _format_size(self, size_in_bytes: int) -> str:
        """Format byte size to human readable format."""
        return naturalsize(size_in_bytes, binary=True)

    def _calculate_speed(self, bytes_processed: int, time_taken: float) -> str:
        """Calculate and format processing speed."""
        if time_taken > 0:
            mb_per_second = (bytes_processed / 1024 / 1024) / time_taken
            return f"{mb_per_second:.2f} MB/s"
        return "N/A"

    def _get_file_hash(self, content: bytes) -> str:
        """Generate SHA-256 hash of file content."""
        return hashlib.sha256(content).hexdigest()

    def _check_if_already_processed(self, file_hash: str, filename: str) -> bool:
        """Check if file has already been processed."""
        try:
            compressed_key = f"{self.compressed_prefix}{filename}"
            try:
                obj = self.s3_client.head_object(Bucket=self.bucket, Key=compressed_key)
                response = self.s3_client.get_object(Bucket=self.bucket, Key=compressed_key)
                compressed_size = response['ContentLength']
                
                original_response = self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=f"{self.indexed_prefix}{filename}"
                )
                original_size = original_response['ContentLength']
                
                if compressed_size >= original_size:
                    self.logger.info(f"Found existing file but it's not optimally compressed: {filename}")
                    return False
                    
                return True
                
            except self.s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return False
                raise e

        except Exception as e:
            self.logger.warning(f"Error checking processed status: {str(e)}")
            return False

    def _update_stats(self, original_size: int, compressed_size: int):
        """Thread-safe update of processing statistics."""
        with self.stats_lock:
            self.total_original_size += original_size
            self.total_compressed_size += compressed_size

    def _get_compression_args(self, compression_level: int) -> List[str]:
        """Get QPDF compression arguments based on level."""
        base_args = ["--compress-streams=y", "--object-streams=generate"]
        
        if compression_level == 1:
            return base_args + ["--compression-level=7"]
        elif compression_level == 2:
            return base_args + [
                "--compression-level=9",
                "--recompress-flate",
                "--optimize-images"
            ]
        else:  # level 3
            return base_args + [
                "--compression-level=9",
                "--recompress-flate",
                "--optimize-images",
                "--remove-page-properties",
                "--linearize",
                "--compress-streams=y",
                "--decode-level=specialized"
            ]

    def compress_pdf(self, pdf_content: bytes, compression_level: int = 3) -> Optional[bytes]:
        """Universal PDF compression."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                input_path = os.path.join(temp_dir, 'input.pdf')
                output_path = os.path.join(temp_dir, 'output.pdf')
                
                # Write input PDF
                with open(input_path, 'wb') as f:
                    f.write(pdf_content)

                # Get compression arguments
                compression_args = self._get_compression_args(compression_level)
                
                # Run compression
                self.logger.info(f"Running compression with args: {' '.join(compression_args)}")
                cmd = ["qpdf"] + compression_args + [input_path, output_path]
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode != 0:
                    self.logger.error(f"QPDF error: {result.stderr}")
                    return pdf_content

                # Read compressed file
                if os.path.exists(output_path):
                    with open(output_path, 'rb') as f:
                        compressed_content = f.read()
                    
                    # Validate compression
                    original_size = len(pdf_content)
                    compressed_size = len(compressed_content)
                    
                    if compressed_size < original_size:
                        reduction = ((original_size - compressed_size) / original_size) * 100
                        self.logger.info(f"Compression successful: {reduction:.1f}% reduction")
                        return compressed_content
                    else:
                        self.logger.info("Compression did not reduce file size")
                        return pdf_content
                else:
                    self.logger.error("Compression failed to create output file")
                    return pdf_content

        except Exception as e:
            self.logger.error(f"Error in compression: {str(e)}")
            return pdf_content

    def process_file(self, filename: str) -> Tuple[bool, str, Dict]:
        """Process a single PDF file."""
        file_start_time = time.time()
        file_stats = {
            'original_size': 0,
            'compressed_size': 0,
            'processing_time': 0,
            'filename': filename
        }
        
        try:
            # Download file
            self.logger.info(f"Starting processing of {filename}")
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=f"{self.indexed_prefix}{filename}"
            )
            pdf_content = response['Body'].read()
            original_size = len(pdf_content)
            file_stats['original_size'] = original_size
            
            # Check for duplicates
            file_hash = self._get_file_hash(pdf_content)
            if self._check_if_already_processed(file_hash, filename):
                return True, f"Skipped {filename} (already processed)", file_stats

            # Compress PDF
            self.logger.info(f"Compressing {filename} (Original size: {self._format_size(original_size)})")
            compressed_content = self.compress_pdf(pdf_content, compression_level=3)
            
            compressed_size = len(compressed_content)
            file_stats['compressed_size'] = compressed_size
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            # Upload compressed file
            compressed_key = f"{self.compressed_prefix}{filename}"
            self.logger.info(f"Uploading compressed {filename}")
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=compressed_key,
                Body=compressed_content,
                ContentType='application/pdf'
            )

            # Update statistics
            self._update_stats(original_size, compressed_size)
            
            # Calculate metrics
            time_taken = time.time() - file_start_time
            file_stats['processing_time'] = time_taken
            processing_speed = self._calculate_speed(original_size, time_taken)

            # Log results
            self.logger.info(
                f"Compressed {filename}:\n"
                f"  Original size: {self._format_size(original_size)}\n"
                f"  Compressed size: {self._format_size(compressed_size)}\n"
                f"  Reduction: {compression_ratio:.1f}%\n"
                f"  Processing time: {time_taken:.2f} seconds\n"
                f"  Speed: {processing_speed}"
            )

            return True, f"Successfully compressed {filename}", file_stats

        except Exception as e:
            error_msg = f"Error processing {filename}: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg, file_stats

    def process_directory(self) -> dict:
        """Process all PDF files in the directory concurrently."""
        self.start_time = time.time()
        self.logger.info(f"Starting PDF compression process with {self.max_workers} workers")
        
        stats = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'errors': [],
            'total_original_size': 0,
            'total_compressed_size': 0,
            'processing_time': 0,
            'file_stats': []
        }

        try:
            # List all PDF files
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.indexed_prefix
            )

            if 'Contents' not in response:
                self.logger.warning("No files found in indexed directory")
                return stats

            # Filter PDF files
            pdf_files = [
                obj['Key'].split('/')[-1] for obj in response['Contents']
                if obj['Key'].lower().endswith('.pdf')
            ]
            
            total_files = len(pdf_files)
            self.logger.info(f"Found {total_files} PDF files to process")

            # Process files concurrently
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_filename = {
                    executor.submit(self.process_file, filename): filename
                    for filename in pdf_files
                }
                
                for future in as_completed(future_to_filename):
                    filename = future_to_filename[future]
                    stats['total'] += 1
                    
                    try:
                        success, message, file_stats = future.result()
                        stats['file_stats'].append(file_stats)
                        
                        if "already processed" in message:
                            stats['skipped'] += 1
                            self.logger.info(f"Skipped {filename} (already processed)")
                        elif success:
                            stats['successful'] += 1
                        else:
                            stats['failed'] += 1
                            stats['errors'].append(message)
                            
                    except Exception as e:
                        stats['failed'] += 1
                        error_msg = f"Error processing {filename}: {str(e)}"
                        stats['errors'].append(error_msg)
                        self.logger.error(error_msg)

            # Calculate final statistics
            total_time = time.time() - self.start_time
            total_reduction = ((1 - self.total_compressed_size / self.total_original_size) * 100) if self.total_original_size > 0 else 0
            overall_speed = self._calculate_speed(self.total_original_size, total_time)

            # Log summary
            self.logger.info(
                f"\nProcessing Summary:\n"
                f"  Total files found: {total_files}\n"
                f"  Successfully compressed: {stats['successful']}\n"
                f"  Skipped (already processed): {stats['skipped']}\n"
                f"  Failed: {stats['failed']}\n"
                f"  Total original size: {self._format_size(self.total_original_size)}\n"
                f"  Total compressed size: {self._format_size(self.total_compressed_size)}\n"
                f"  Overall reduction: {total_reduction:.1f}%\n"
                f"  Total processing time: {total_time:.2f} seconds\n"
                f"  Average processing speed: {overall_speed}"
            )

            stats['total_original_size'] = self.total_original_size
            stats['total_compressed_size'] = self.total_compressed_size
            stats['processing_time'] = total_time

        except Exception as e:
            self.logger.error(f"Error processing directory: {str(e)}")
            stats['errors'].append(f"Directory processing error: {str(e)}")

        return stats


def compress_indexed_pdfs(env_vars: dict, max_workers: int = 4) -> dict:
    """Main function to compress PDFs."""
    compressor = PDFCompressor(
        aws_access_key_id=env_vars['S3_ACCESS_KEY_ID'],
        aws_secret_access_key=env_vars['S3_SECRET_ACCESS_KEY'],
        region=env_vars['S3_REGION'],
        bucket=env_vars['S3_BUCKET'],
        indexed_prefix=env_vars['S3_INDEXED_PREFIX'],
        compressed_prefix=env_vars['S3_COMPRESSED_PREFIX'],
        max_workers=max_workers
    )
    
    return compressor.process_directory()


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Environment variables
    env_vars = {
        'S3_ACCESS_KEY_ID': os.getenv('S3_ACCESS_KEY_ID'),
        'S3_SECRET_ACCESS_KEY': os.getenv('S3_SECRET_ACCESS_KEY'),
        'S3_REGION': os.getenv('S3_REGION'),
        'S3_BUCKET': os.getenv('S3_BUCKET'),
        'S3_INDEXED_PREFIX': os.getenv('S3_INDEXED_PREFIX'),
        'S3_COMPRESSED_PREFIX': os.getenv('S3_COMPRESSED_PREFIX')
    }

    # Verify environment variables
    required_vars = [
        'S3_ACCESS_KEY_ID', 
        'S3_SECRET_ACCESS_KEY', 
        'S3_REGION', 
        'S3_BUCKET',
        'S3_INDEXED_PREFIX',
        'S3_COMPRESSED_PREFIX'
    ]
    
    missing_vars = [var for var in required_vars if not env_vars.get(var)]
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        exit(1)

    # Print configuration
    print("Configuration:")
    print(f"Bucket: {env_vars['S3_BUCKET']}")
    print(f"Indexed prefix: {env_vars['S3_INDEXED_PREFIX']}")
    print(f"Compressed prefix: {env_vars['S3_COMPRESSED_PREFIX']}")

    try:
        # Set number of workers based on CPU cores
        import multiprocessing
        recommended_workers = max(1, multiprocessing.cpu_count() - 1)
        print(f"\nUsing {recommended_workers} workers")
        
        # Run compression
        stats = compress_indexed_pdfs(env_vars, max_workers=recommended_workers)
        
        # Print any errors that occurred
        if stats['errors']:
            print("\nErrors encountered:")
            for error in stats['errors']:
                print(f"- {error}")
                
        # Print final statistics
        print("\nFinal Statistics:")
        print(f"Total files processed: {stats['total']}")
        print(f"Successfully compressed: {stats['successful']}")
        print(f"Skipped (already processed): {stats['skipped']}")
        print(f"Failed: {stats['failed']}")
        print(f"Total processing time: {stats['processing_time']:.2f} seconds")
        
        if stats['total_original_size'] > 0:
            reduction = ((stats['total_original_size'] - stats['total_compressed_size']) / 
                        stats['total_original_size'] * 100)
            print(f"Overall space reduction: {reduction:.1f}%")
            print(f"Space saved: {naturalsize(stats['total_original_size'] - stats['total_compressed_size'], binary=True)}")
        
    except Exception as e:
        print(f"Error running compression: {str(e)}")