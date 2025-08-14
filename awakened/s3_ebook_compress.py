import os
import sys
import boto3
import pikepdf
from PIL import Image
import io
from typing import Optional, Tuple, List, Dict
import logging
from datetime import datetime
import time
from humanize import naturalsize
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from threading import Lock, Event
import tempfile
import subprocess
import signal
import zipfile
import shutil

class EbookCompressor:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region: str,
                 bucket: str, indexed_prefix: str, compressed_prefix: str, max_workers: int = 4):
        """Initialize the ebook compressor with AWS credentials and S3 configuration."""
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
        self.shutdown_flag = Event()
        
        # Configure logging with timestamp format
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Thread-safe counters using Lock
        self.stats_lock = Lock()
        self.total_original_size = 0
        self.total_compressed_size = 0
        self.start_time = None
        
        # Cache for processed files to prevent duplicates
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
        """Check if file has already been processed using both hash and filename."""
        try:
            # First check if the destination prefix exists
            try:
                self.s3_client.head_object(
                    Bucket=self.bucket,
                    Key=f"{self.compressed_prefix}"
                )
            except:
                # If prefix doesn't exist, create it
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=self.compressed_prefix,
                    Body=''
                )
                return False

            # Check if file exists in compressed directory
            compressed_key = f"{self.compressed_prefix}{filename}"
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=compressed_key)
                compressed_size = response['ContentLength']
                
                # Get original file size
                original_response = self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=f"{self.indexed_prefix}{filename}"
                )
                original_size = original_response['ContentLength']
                
                # If the "compressed" file is the same size or larger, we should reprocess it
                if compressed_size >= original_size:
                    self.logger.info(f"Found existing file but it's not compressed properly: {filename}")
                    return False
                    
                return True
                
            except self.s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return False
                else:
                    raise e

        except Exception as e:
            self.logger.warning(f"Error checking processed status: {str(e)}")
            return False

    def _update_stats(self, original_size: int, compressed_size: int):
        """Thread-safe update of processing statistics."""
        with self.stats_lock:
            self.total_original_size += original_size
            self.total_compressed_size += compressed_size

    def _analyze_pdf_content(self, pdf_content: bytes) -> dict:
        """Analyze PDF content to determine the best compression strategy."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = os.path.join(temp_dir, 'temp.pdf')
                with open(temp_path, 'wb') as f:
                    f.write(pdf_content)
                    
                pdf = pikepdf.Pdf.open(temp_path)
                
                analysis = {
                    'total_pages': len(pdf.pages),
                    'has_images': False,
                    'image_count': 0,
                    'has_text': False,
                    'file_size': len(pdf_content),
                    'is_scanned': False
                }
                
                # Sample pages for content analysis
                pages_to_check = min(3, analysis['total_pages'])
                for i in range(pages_to_check):
                    page = pdf.pages[i]
                    if '/Font' in page.Resources.get('/Resources', {}):
                        analysis['has_text'] = True
                    if hasattr(page, 'images') and page.images:
                        analysis['has_images'] = True
                        analysis['image_count'] += len(page.images)
                        
                # Check if it might be a scanned document
                analysis['is_scanned'] = (analysis['has_images'] and 
                                        analysis['image_count'] >= analysis['total_pages'] and 
                                        not analysis['has_text'])
                        
                return analysis
        except Exception as e:
            self.logger.warning(f"Content analysis failed: {str(e)}")
            return {
                'total_pages': 0,
                'has_images': True,
                'has_text': True,
                'file_size': len(pdf_content),
                'is_scanned': False
            }

    def _compress_regular_pdf(self, input_path: str, output_path: str, level: int) -> bytes:
        """Compress regular PDFs with text and possibly some images."""
        try:
            # QPDF for regular PDFs
            compression_args = [
                "--compress-streams=y",
                "--object-streams=generate",
                f"--compression-level={min(level * 3, 9)}",
            ]
            
            if level >= 2:
                compression_args.extend([
                    "--recompress-flate",
                    "--optimize-images"
                ])
                
            cmd = ["qpdf"] + compression_args + [input_path, output_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.warning(f"QPDF command failed: {result.stderr}")
                return self._fallback_compression(input_path, output_path)
            
            with open(output_path, 'rb') as f:
                return f.read()
                
        except Exception as e:
            self.logger.error(f"Regular compression failed: {str(e)}")
            return self._fallback_compression(input_path, output_path)

    def _fallback_compression(self, input_path: str, output_path: str) -> bytes:
        """Fallback compression using pikepdf when qpdf fails."""
        try:
            pdf = pikepdf.Pdf.open(input_path)
            pdf.save(output_path,
                    compress_streams=True,
                    object_stream_mode=pikepdf.ObjectStreamMode.generate)
            with open(output_path, 'rb') as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"Fallback compression failed: {str(e)}")
            with open(input_path, 'rb') as f:
                return f.read()

    def _process_image(self, img, max_dimension: int, quality: int) -> Optional[pikepdf.Image]:
        """Process a single image with error handling."""
        try:
            # Try different methods to convert image
            try:
                pil_img = img.as_pil_image()
            except Exception:
                try:
                    raw_bytes = img.read_raw_bytes()
                    pil_img = Image.open(io.BytesIO(raw_bytes))
                except Exception as img_err:
                    self.logger.warning(f"Cannot process image: {str(img_err)}")
                    return None

            # Convert to RGB if needed
            if pil_img.mode == 'RGBA':
                pil_img = pil_img.convert('RGB')
            elif pil_img.mode not in ['RGB', 'L']:
                pil_img = pil_img.convert('RGB')

            # Resize if needed
            if pil_img.width > max_dimension or pil_img.height > max_dimension:
                ratio = min(max_dimension/pil_img.width, max_dimension/pil_img.height)
                new_size = (int(pil_img.width * ratio), int(pil_img.height * ratio))
                pil_img = pil_img.resize(new_size, Image.Resampling.LANCZOS)

            with tempfile.NamedTemporaryFile(suffix='.jpg') as tmp:
                pil_img.save(tmp.name, 'JPEG', quality=quality, optimize=True)
                return pikepdf.Image.open(tmp.name)

        except Exception as e:
            self.logger.warning(f"Image processing error: {str(e)}")
            return None

    def _compress_image_heavy_pdf(self, input_path: str, output_path: str, level: int) -> bytes:
        """Compress PDFs with many images."""
        try:
            # First try basic compression
            basic_compressed = self._compress_regular_pdf(input_path, output_path, level)
            
            # If basic compression didn't achieve at least 10% reduction, try more aggressive compression
            if len(basic_compressed) > 0.9 * os.path.getsize(input_path):
                self.logger.info("Basic compression insufficient, trying aggressive compression...")
                
                with pikepdf.Pdf.open(input_path) as pdf:
                    for page in pdf.pages:
                        if hasattr(page, 'images'):
                            for img_key, img in page.images.items():
                                max_dimension = 1500 if level >= 2 else 2000
                                quality = 60 if level >= 2 else 75
                                
                                new_image = self._process_image(img, max_dimension, quality)
                                if new_image is not None:
                                    page.images[img_key] = new_image
                    
                    pdf.save(output_path,
                            compress_streams=True,
                            object_stream_mode=pikepdf.ObjectStreamMode.generate,
                            normalize_content=True if level >= 2 else False)
                
                with open(output_path, 'rb') as f:
                    aggressive_compressed = f.read()
                    
                # Return the smaller of the two compressions
                if len(aggressive_compressed) < len(basic_compressed):
                    return aggressive_compressed
            
            return basic_compressed
                    
        except Exception as e:
            self.logger.error(f"Image-heavy compression failed: {str(e)}")
            return self._fallback_compression(input_path, output_path)

    def _compress_scanned_pdf(self, input_path: str, output_path: str, level: int) -> bytes:
        """Special handling for scanned PDFs."""
        try:
            quality = {1: 90, 2: 80, 3: 70}[level]
            max_dimension = {1: 2400, 2: 2000, 3: 1800}[level]
            
            with pikepdf.Pdf.open(input_path) as pdf:
                for page in pdf.pages:
                    if hasattr(page, 'images'):
                        for img_key, img in page.images.items():
                            new_image = self._process_image(img, max_dimension, quality)
                            if new_image is not None:
                                page.images[img_key] = new_image
                
                pdf.save(output_path,
                        compress_streams=True,
                        object_stream_mode=pikepdf.ObjectStreamMode.generate)
            
            with open(output_path, 'rb') as f:
                return f.read()
                
        except Exception as e:
            self.logger.error(f"Scanned PDF compression failed: {str(e)}")
            return self._fallback_compression(input_path, output_path)

    def _compress_epub(self, epub_content: bytes, compression_level: int = 3) -> Optional[bytes]:
        """
        Compress EPUB file by optimizing images and recompressing the ZIP structure.
        Args:
            epub_content: Raw EPUB bytes
            compression_level: Compression level (1-3), higher means more compression
        Returns:
            Compressed EPUB bytes or None if compression fails
        """
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Create paths for working with the EPUB
                input_epub = os.path.join(temp_dir, 'input.epub')
                extracted_dir = os.path.join(temp_dir, 'extracted')
                output_epub = os.path.join(temp_dir, 'output.epub')
                
                # Write input EPUB to temporary file
                with open(input_epub, 'wb') as f:
                    f.write(epub_content)
                
                # Extract EPUB contents (it's basically a ZIP file)
                with zipfile.ZipFile(input_epub, 'r') as zip_ref:
                    zip_ref.extractall(extracted_dir)
                
                # Process images in the EPUB
                for root, _, files in os.walk(extracted_dir):
                    for file in files:
                        if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                            file_path = os.path.join(root, file)
                            try:
                                # Open and optimize image
                                with Image.open(file_path) as img:
                                    # Convert RGBA to RGB if needed
                                    if img.mode == 'RGBA':
                                        img = img.convert('RGB')
                                    
                                    # Determine compression parameters based on level
                                    quality = {1: 90, 2: 80, 3: 70}[compression_level]
                                    max_dimension = {1: 2400, 2: 2000, 3: 1800}[compression_level]
                                    
                                    # Resize if needed
                                    if img.width > max_dimension or img.height > max_dimension:
                                        ratio = min(max_dimension/img.width, max_dimension/img.height)
                                        new_size = (int(img.width * ratio), int(img.height * ratio))
                                        img = img.resize(new_size, Image.Resampling.LANCZOS)
                                    
                                    # Save optimized image
                                    img.save(file_path, quality=quality, optimize=True)
                            except Exception as e:
                                self.logger.warning(f"Failed to optimize image {file}: {str(e)}")
                
                # Create new EPUB with maximum ZIP compression
                with zipfile.ZipFile(output_epub, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zip_ref:
                    for root, _, files in os.walk(extracted_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, extracted_dir)
                            zip_ref.write(file_path, arcname)
                
                # Read the compressed EPUB
                with open(output_epub, 'rb') as f:
                    compressed_content = f.read()
                
                return self._verify_compression(epub_content, compressed_content)
                
        except Exception as e:
            self.logger.error(f"EPUB compression failed: {str(e)}")
            return epub_content

    def _verify_compression(self, original_content: bytes, compressed_content: bytes) -> bytes:
        """Verify compression result and return the better version."""
        original_size = len(original_content)
        compressed_size = len(compressed_content)
        
        if compressed_size < original_size:
            reduction = ((original_size - compressed_size) / original_size) * 100
            self.logger.info(f"Compression successful: {reduction:.1f}% reduction")
            return compressed_content
        else:
            self.logger.info("Compression did not reduce file size, keeping original")
            return original_content

    def compress_pdf(self, pdf_content: bytes, compression_level: int = 3) -> Optional[bytes]:
        """
        Smart PDF compression using the best method based on content.
        Args:
            pdf_content: Raw PDF bytes
            compression_level: Compression level (1-3), higher means more compression
        Returns:
            Compressed PDF bytes or None if compression fails
        """
        try:
            # Analyze content to determine best strategy
            analysis = self._analyze_pdf_content(pdf_content)
            self.logger.info(f"PDF Analysis: {analysis}")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                input_path = os.path.join(temp_dir, 'input.pdf')
                output_path = os.path.join(temp_dir, 'output.pdf')
                
                with open(input_path, 'wb') as f:
                    f.write(pdf_content)
                
                if analysis['is_scanned']:
                    compressed = self._compress_scanned_pdf(input_path, output_path, compression_level)
                elif analysis['has_images'] and analysis['file_size'] > 1_000_000:
                    compressed = self._compress_image_heavy_pdf(input_path, output_path, compression_level)
                else:
                    compressed = self._compress_regular_pdf(input_path, output_path, compression_level)
                
                return self._verify_compression(pdf_content, compressed)
        
        except Exception as e:
            self.logger.error(f"Error in compression: {str(e)}")
            return pdf_content

    def compress_file(self, content: bytes, filename: str, compression_level: int = 3) -> Optional[bytes]:
        """
        Smart compression using the best method based on file type.
        Args:
            content: Raw file bytes
            filename: Name of the file (used to determine type)
            compression_level: Compression level (1-3), higher means more compression
        Returns:
            Compressed file bytes or None if compression fails
        """
        try:
            if filename.lower().endswith('.pdf'):
                return self.compress_pdf(content, compression_level)
            elif filename.lower().endswith('.epub'):
                return self._compress_epub(content, compression_level)
            else:
                self.logger.warning(f"Unsupported file type: {filename}")
                return content
                
        except Exception as e:
            self.logger.error(f"Error in compression: {str(e)}")
            return content

    def process_file(self, filename: str) -> Tuple[bool, str, Dict]:
        """
        Process a single file: download, compress, and upload.
        Args:
            filename: Name of the file in the indexed directory
        Returns:
            Tuple of (success_status, message, stats)
        """
        file_start_time = time.time()
        file_stats = {
            'original_size': 0,
            'compressed_size': 0,
            'processing_time': 0,
            'filename': filename
        }
        
        try:
            if self.shutdown_flag.is_set():
                return False, "Processing interrupted", file_stats

            # Download file from indexed directory
            self.logger.info(f"Starting processing of {filename}")
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=f"{self.indexed_prefix}{filename}"
            )
            content = response['Body'].read()
            original_size = len(content)
            file_stats['original_size'] = original_size
            
            # Check for duplicates using content hash
            file_hash = self._get_file_hash(content)
            if self._check_if_already_processed(file_hash, filename):
                return True, f"Skipped {filename} (already processed)", file_stats

            # Compress the file
            self.logger.info(f"Compressing {filename} (Original size: {self._format_size(original_size)})")
            compressed_content = self.compress_file(content, filename)
            if compressed_content is None:
                return False, f"Failed to compress {filename}", file_stats

            compressed_size = len(compressed_content)
            file_stats['compressed_size'] = compressed_size
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            if self.shutdown_flag.is_set():
                return False, "Processing interrupted", file_stats

            # Upload compressed file
            compressed_key = f"{self.compressed_prefix}{filename}"
            content_type = 'application/pdf' if filename.lower().endswith('.pdf') else 'application/epub+zip'
            self.logger.info(f"Uploading compressed {filename}")
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=compressed_key,
                Body=compressed_content,
                ContentType=content_type
            )

            # Update total statistics in a thread-safe manner
            self._update_stats(original_size, compressed_size)
            
            # Calculate processing metrics
            time_taken = time.time() - file_start_time
            file_stats['processing_time'] = time_taken
            processing_speed = self._calculate_speed(original_size, time_taken)

            # Log detailed compression results
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
        """
        Process all PDF and EPUB files in the indexed directory concurrently.
        Returns:
            Dictionary with processing statistics
        """
        self.start_time = time.time()
        self.logger.info(f"Starting ebook compression process with {self.max_workers} workers")
        
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
            # List all PDF and EPUB files in indexed directory
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.indexed_prefix
            )

            if 'Contents' not in response:
                self.logger.warning("No files found in indexed directory")
                return stats

            # Filter PDF and EPUB files
            ebook_files = [
                obj['Key'].split('/')[-1] for obj in response['Contents']
                if obj['Key'].lower().endswith(('.pdf', '.epub'))
            ]
            
            total_files = len(ebook_files)
            self.logger.info(f"Found {total_files} ebook files to process")

            # Process files concurrently
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                future_to_filename = {}
                
                # Submit all tasks
                for filename in ebook_files:
                    if self.shutdown_flag.is_set():
                        break
                    future = executor.submit(self.process_file, filename)
                    futures.append(future)
                    future_to_filename[future] = filename
                
                # Process completed tasks as they finish
                for future in as_completed(futures):
                    if self.shutdown_flag.is_set():
                        executor.shutdown(wait=False)
                        break
                        
                    filename = future_to_filename[future]
                    stats['total'] += 1
                    
                    try:
                        success, message, file_stats = future.result(timeout=300)  # 5 minute timeout
                        stats['file_stats'].append(file_stats)
                        
                        if "already processed" in message:
                            stats['skipped'] += 1
                            self.logger.info(f"Skipped {filename} (already processed)")
                        elif success:
                            stats['successful'] += 1
                        else:
                            stats['failed'] += 1
                            stats['errors'].append(message)
                            
                    except TimeoutError:
                        stats['failed'] += 1
                        error_msg = f"Timeout processing {filename}"
                        stats['errors'].append(error_msg)
                        self.logger.error(error_msg)
                    except Exception as e:
                        stats['failed'] += 1
                        error_msg = f"Error processing {filename}: {str(e)}"
                        stats['errors'].append(error_msg)
                        self.logger.error(error_msg)

            # Calculate final statistics
            total_time = time.time() - self.start_time
            if self.total_original_size > 0:
                total_reduction = ((1 - self.total_compressed_size / self.total_original_size) * 100)
            else:
                total_reduction = 0
            overall_speed = self._calculate_speed(self.total_original_size, total_time)

            # Log final summary
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

            # Update final statistics
            stats['total_original_size'] = self.total_original_size
            stats['total_compressed_size'] = self.total_compressed_size
            stats['processing_time'] = total_time

        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal, cleaning up...")
            self.shutdown_flag.set()
            # Let current operations finish
            time.sleep(2)
            return stats
        except Exception as e:
            self.logger.error(f"Error processing directory: {str(e)}")
            stats['errors'].append(f"Directory processing error: {str(e)}")

        return stats


def compress_indexed_ebooks(env_vars: dict, max_workers: int = 4) -> dict:
    """
    Main function to compress ebooks from indexed directory and upload to compressed directory.
    Args:
        env_vars: Dictionary containing environment variables
        max_workers: Maximum number of concurrent workers
    Returns:
        Processing statistics
    """
    compressor = EbookCompressor(
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
    import os
    from dotenv import load_dotenv
    import signal
    
    def signal_handler(signum, frame):
        print("\nReceived shutdown signal. Cleaning up...")
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    
    # Load environment variables from .env file
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

    # Verify all required environment variables are present
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

    # Print configuration (without sensitive data)
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
        stats = compress_indexed_ebooks(env_vars, max_workers=recommended_workers)
        
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
        
    except Exception as e:
        print(f"Error running compression: {str(e)}")