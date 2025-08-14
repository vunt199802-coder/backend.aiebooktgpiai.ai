import os
import tempfile
import subprocess
import logging
from datetime import datetime
import time
from humanize import naturalsize
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from threading import Lock
from typing import Optional, Tuple, Dict
import zipfile
from PIL import Image
import io
import shutil

class LocalEbookCompressor:
    def __init__(self, pdf_dir: str, epub_dir: str, output_dir: str, max_workers: int = 4):
        """Initialize the Ebook compressor with local directories."""
        self.pdf_dir = os.path.expanduser(pdf_dir)
        self.epub_dir = os.path.expanduser(epub_dir)
        self.output_dir = os.path.expanduser(output_dir)
        self.max_workers = max_workers
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
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
        return naturalsize(size_in_bytes)

    def _calculate_speed(self, bytes_processed: int, time_taken: float) -> str:
        """Calculate and format processing speed."""
        if time_taken == 0:
            return "N/A"
        speed = bytes_processed / time_taken / 1024 / 1024  # MB/s
        return f"{speed:.2f} MB/s"

    def _get_file_hash(self, content: bytes) -> str:
        """Generate SHA-256 hash of file content."""
        return hashlib.sha256(content).hexdigest()

    def _check_if_already_processed(self, file_hash: str, filename: str) -> bool:
        """Check if file has already been processed."""
        output_path = os.path.join(self.output_dir, filename)
        if os.path.exists(output_path):
            self.logger.info(f"File {filename} already exists in output directory")
            return True
        return False

    def _update_stats(self, original_size: int, compressed_size: int):
        """Thread-safe update of processing statistics."""
        with self.stats_lock:
            self.total_original_size += original_size
            self.total_compressed_size += compressed_size

    def compress_pdf(self, pdf_content: bytes, compression_level: int = 3) -> Optional[bytes]:
        """Compress PDF using qpdf with improved error handling and adaptive compression strategies."""
        input_path = None
        output_path = None
        temp_dir = None
        
        try:
            # Create a temporary directory
            temp_dir = tempfile.mkdtemp()
            input_path = os.path.join(temp_dir, 'input.pdf')
            output_path = os.path.join(temp_dir, 'output.pdf')
            
            # Write input file
            with open(input_path, 'wb') as f:
                f.write(pdf_content)
            
            original_size = len(pdf_content)
            best_size = original_size
            best_content = pdf_content
            
            # Analyze file size to determine initial strategy
            size_mb = original_size / (1024 * 1024)
            
            # Compression strategies from conservative to aggressive
            strategies = [
                # Conservative - for very small files
                ['qpdf', '--stream-data=compress', '--compress-streams=y', '--object-streams=generate'],
                
                # Aggressive - default strategy for most files
                ['qpdf', '--stream-data=compress', '--compress-streams=y', '--object-streams=generate',
                 '--decode-level=specialized', '--normalize-content=y', '--remove-page-labels',
                 '--compress-streams=y', '--recompress-flate', '--compression-level=9'],
                
                # Maximum - for files needing extra optimization
                ['qpdf', '--stream-data=compress', '--compress-streams=y', '--object-streams=generate',
                 '--decode-level=specialized', '--normalize-content=y', '--remove-page-labels',
                 '--compress-streams=y', '--recompress-flate', '--compression-level=9',
                 '--optimize-images']
            ]
            
            # Start with aggressive compression for most files
            start_index = 1  # Default to aggressive compression
            if size_mb <= 1:  # Only use conservative for very small files
                start_index = 0
            
            # Try more aggressive compression for files with low initial compression
            min_reduction_threshold = 5.0  # Minimum reduction percentage to accept
            
            for strategy_index, strategy in enumerate(strategies[start_index:], start=start_index):
                try:
                    cmd = strategy + [input_path, output_path]
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)  # Increased timeout for aggressive compression
                    
                    # Check for critical errors
                    error_output = result.stderr.lower()
                    if ("error" in error_output and 
                        not any(safe_warning in error_output for safe_warning in 
                            ["warning", "offset 0", "no objects", "invalid"])):
                        self.logger.warning(f"Strategy {strategy_index} failed with error: {error_output}")
                        continue
                    
                    # Verify output exists and is valid
                    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                        self.logger.warning(f"Strategy {strategy_index} produced empty or missing output file")
                        continue
                        
                    # Check compression results
                    compressed_size = os.path.getsize(output_path)
                    if compressed_size < best_size:
                        with open(output_path, 'rb') as f:
                            compressed_content = f.read()
                            
                        # Verify the compressed content is valid PDF
                        if not compressed_content.startswith(b'%PDF-'):
                            self.logger.warning("Output is not a valid PDF")
                            continue
                            
                        reduction = ((original_size - compressed_size) / original_size) * 100
                        self.logger.info(f"Strategy {strategy_index} achieved {reduction:.1f}% reduction")
                        
                        # Update best result
                        best_size = compressed_size
                        best_content = compressed_content
                        
                        # Early exit only if we achieve very good compression
                        if reduction >= 30:  # Increased threshold for early exit
                            break
                            
                        # Try maximum compression if reduction is too low
                        if reduction < min_reduction_threshold and strategy_index < len(strategies) - 1:
                            continue
                            
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Strategy {strategy_index} timed out after 120 seconds")
                    continue
                except Exception as e:
                    self.logger.warning(f"Strategy {strategy_index} failed: {str(e)}")
                    continue
                finally:
                    # Clean up output file for next strategy
                    if os.path.exists(output_path):
                        try:
                            os.unlink(output_path)
                        except Exception:
                            pass
            
            if best_size < original_size:
                return best_content
            else:
                self.logger.info("No effective compression found, using original")
                return pdf_content
                
        except Exception as e:
            self.logger.error(f"PDF compression failed: {str(e)}")
            return pdf_content
            
        finally:
            # Clean up temp directory
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except Exception as e:
                    self.logger.warning(f"Failed to remove temporary directory: {str(e)}")

    def _compress_epub(self, epub_content: bytes, compression_level: int = 3) -> bytes:
        """Compress EPUB file by optimizing images and recompressing the ZIP structure."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                input_epub = os.path.join(temp_dir, 'input.epub')
                extract_dir = os.path.join(temp_dir, 'extracted')
                output_epub = os.path.join(temp_dir, 'output.epub')
                
                # Write input EPUB
                with open(input_epub, 'wb') as f:
                    f.write(epub_content)
                
                # Extract EPUB contents
                with zipfile.ZipFile(input_epub, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                # Process images
                for root, _, files in os.walk(extract_dir):
                    for file in files:
                        if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                            file_path = os.path.join(root, file)
                            try:
                                with Image.open(file_path) as img:
                                    if img.mode == 'RGBA':
                                        img = img.convert('RGB')
                                    
                                    output = io.BytesIO()
                                    img.save(output, format=img.format, optimize=True, quality=85)
                                    
                                    with open(file_path, 'wb') as f:
                                        f.write(output.getvalue())
                            except Exception as e:
                                self.logger.warning(f"Failed to optimize image {file}: {str(e)}")
                
                # Create new EPUB with maximum compression
                with zipfile.ZipFile(output_epub, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zip_ref:
                    for root, _, files in os.walk(extract_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, extract_dir)
                            zip_ref.write(file_path, arcname)
                
                # Read compressed EPUB
                with open(output_epub, 'rb') as f:
                    compressed_content = f.read()
                
                return self._verify_compression(epub_content, compressed_content)
            
        except Exception as e:
            self.logger.error(f"EPUB compression failed: {str(e)}")
            return epub_content

    def _verify_compression(self, original_content: bytes, compressed_content: bytes) -> bytes:
        """Verify if the compressed content is smaller than the original."""
        original_size = len(original_content)
        compressed_size = len(compressed_content)
        
        if compressed_size < original_size:
            reduction = ((original_size - compressed_size) / original_size) * 100
            self.logger.info(f"Compression successful: {reduction:.1f}% reduction")
            return compressed_content
        else:
            self.logger.info("Compression did not reduce file size")
            return original_content

    def process_file(self, filepath: str) -> Tuple[bool, str, Dict]:
        """Process a single file."""
        filename = os.path.basename(filepath)
        output_path = os.path.join(self.output_dir, filename)
        file_start_time = time.time()
        file_stats = {
            'filename': filename,
            'original_size': 0,
            'compressed_size': 0,
            'processing_time': 0,
            'speed': 0,
            'reduction': 0
        }

        try:
            # Read original file
            with open(filepath, 'rb') as f:
                content = f.read()
            original_size = len(content)
            file_stats['original_size'] = original_size

            # Check if file was already processed
            file_hash = self._get_file_hash(content)
            if self._check_if_already_processed(file_hash, filename):
                # Still update stats for existing compressed file
                if os.path.exists(output_path):
                    compressed_size = os.path.getsize(output_path)
                    self._update_stats(original_size, compressed_size)
                    reduction = ((original_size - compressed_size) / original_size) * 100
                    self.logger.info(f"File {filename} already exists in output directory with {reduction:.1f}% reduction")
                return True, "already processed", file_stats

            # Compress based on file type
            if filepath.lower().endswith('.pdf'):
                compressed_content = self.compress_pdf(content)
            elif filepath.lower().endswith('.epub'):
                compressed_content = self._compress_epub(content)
            else:
                self.logger.warning(f"Unsupported file type: {filepath}")
                return False, filename, file_stats

            if compressed_content is None:
                self.logger.error(f"Compression failed for {filename}")
                return False, filename, file_stats

            compressed_size = len(compressed_content)
            processing_time = time.time() - file_start_time

            # Update stats regardless of compression result
            self._update_stats(original_size, compressed_size)
            
            # Write output file and log results
            if compressed_size < original_size:
                with open(output_path, 'wb') as f:
                    f.write(compressed_content)
                
                reduction = ((original_size - compressed_size) / original_size) * 100
                speed = original_size / processing_time / 1024 / 1024  # MB/s
                
                self.logger.info(f"\nProcessed {filename}:")
                self.logger.info(f"  Original size: {self._format_size(original_size)}")
                self.logger.info(f"  Compressed size: {self._format_size(compressed_size)}")
                self.logger.info(f"  Reduction: {reduction:.1f}%")
                self.logger.info(f"  Processing time: {processing_time:.2f} seconds")
                self.logger.info(f"  Speed: {speed:.2f} MB/s")
                
                file_stats.update({
                    'compressed_size': compressed_size,
                    'processing_time': processing_time,
                    'speed': speed,
                    'reduction': reduction
                })
            else:
                self.logger.info("Compression did not reduce file size")
                # Copy original file if no compressed version exists
                if not os.path.exists(output_path):
                    with open(output_path, 'wb') as f:
                        f.write(content)
                    compressed_size = original_size
                    file_stats['compressed_size'] = compressed_size
            
            return True, filename, file_stats
                
        except Exception as e:
            self.logger.error(f"Error processing {filename}: {str(e)}")
            return False, filename, file_stats

    def process_directories(self) -> dict:
        """Process all files in the directories concurrently."""
        self.start_time = time.time()
        self.total_original_size = 0  # Reset counters
        self.total_compressed_size = 0
        
        self.logger.info(f"Starting file compression process with {self.max_workers} workers")
        
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
            # Get all PDF and EPUB files
            pdf_files = [os.path.join(self.pdf_dir, f) for f in os.listdir(self.pdf_dir)
                        if f.lower().endswith('.pdf')]
            epub_files = [os.path.join(self.epub_dir, f) for f in os.listdir(self.epub_dir)
                         if f.lower().endswith('.epub')]
            
            all_files = pdf_files + epub_files
            total_files = len(all_files)
            
            self.logger.info(f"Found {len(pdf_files)} PDF files and {len(epub_files)} EPUB files")

            # Process files concurrently
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_filepath = {
                    executor.submit(self.process_file, filepath): filepath
                    for filepath in all_files
                }
                
                for future in as_completed(future_to_filepath):
                    filepath = future_to_filepath[future]
                    filename = os.path.basename(filepath)
                    stats['total'] += 1
                    
                    try:
                        success, message, file_stats = future.result()
                        stats['file_stats'].append(file_stats)
                        
                        if message == "already processed":
                            stats['skipped'] += 1
                        elif success:
                            stats['successful'] += 1
                        else:
                            stats['failed'] += 1
                            stats['errors'].append(f"Failed to process {filename}")
                            
                    except Exception as e:
                        stats['failed'] += 1
                        stats['errors'].append(f"Error processing {filename}: {str(e)}")
                        self.logger.error(f"Error processing {filename}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error during directory processing: {str(e)}")
            
        finally:
            # Calculate final statistics
            processing_time = time.time() - self.start_time
            
            stats.update({
                'total_original_size': self.total_original_size,
                'total_compressed_size': self.total_compressed_size,
                'processing_time': processing_time
            })
            
            # Log final statistics
            self.logger.info("\nProcessing Summary:")
            self.logger.info(f"  Total files found: {stats['total']}")
            self.logger.info(f"  Successfully compressed: {stats['successful']}")
            self.logger.info(f"  Skipped (already processed): {stats['skipped']}")
            self.logger.info(f"  Failed: {stats['failed']}")
            self.logger.info(f"  Total original size: {self._format_size(self.total_original_size)}")
            self.logger.info(f"  Total compressed size: {self._format_size(self.total_compressed_size)}")
            
            if self.total_original_size > 0:
                reduction = ((self.total_original_size - self.total_compressed_size) / self.total_original_size) * 100
                self.logger.info(f"  Overall reduction: {reduction:.1f}%")
            else:
                self.logger.info("  Overall reduction: 0.0%")
                
            self.logger.info(f"  Total processing time: {processing_time:.2f} seconds")
            
            if processing_time > 0:
                avg_speed = (self.total_original_size / processing_time) / (1024 * 1024)  # MB/s
                self.logger.info(f"  Average processing speed: {avg_speed:.2f} MB/s")
            else:
                self.logger.info("  Average processing speed: 0.00 MB/s")
            
            if stats['errors']:
                self.logger.info("\nErrors encountered:")
                for error in stats['errors']:
                    self.logger.info(f"  {error}")
                    
            return stats


if __name__ == "__main__":
    # Configure paths
    pdf_dir = "/Users/ifzat/Downloads/Ebook/Ebook/pdf"
    epub_dir = "/Users/ifzat/Downloads/Ebook/Ebook/epub"
    output_dir = "/Users/ifzat/Downloads/Ebook/Ebook/compressed"
    
    try:
        # Set number of workers based on CPU cores
        import multiprocessing
        recommended_workers = max(1, multiprocessing.cpu_count() - 1)
        print(f"\nUsing {recommended_workers} workers")
        
        # Initialize and run compressor
        compressor = LocalEbookCompressor(
            pdf_dir=pdf_dir,
            epub_dir=epub_dir,
            output_dir=output_dir,
            max_workers=recommended_workers
        )
        
        stats = compressor.process_directories()
        
        # Print any errors that occurred
        if stats['errors']:
            print("\nErrors encountered:")
            for error in stats['errors']:
                print(f"- {error}")
                
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)
