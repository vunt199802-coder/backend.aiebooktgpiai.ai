import os
import boto3
import glob
import time
import traceback
import tempfile
import asyncio
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from openai import OpenAI
from datetime import datetime, timedelta
from dotenv import load_dotenv
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import logging
from rich.console import Console
from rich.panel import Panel

# Initialize console for nice output
console = Console()
def log_info(msg): console.print(f"[blue]ℹ️ {msg}[/blue]")
def log_success(msg): console.print(f"[green]✅ {msg}[/green]")
def log_error(msg): console.print(f"[red]❌ {msg}[/red]")
def log_warning(msg): console.print(f"[yellow]⚠️ {msg}[/yellow]")

class ProcessingStatus:
    def __init__(self):
        self.layout = Layout()
        self.layout.split_column(
            Layout(name="header"),
            Layout(name="body"),
            Layout(name="footer")
        )
        
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
        )
        
        self.start_time = time.time()
        self.completed = 0
        self.failed = 0
        self.total_files = 0

    def create_status_table(self):
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        
        elapsed = time.time() - self.start_time
        table.add_row("Time Elapsed", str(timedelta(seconds=int(elapsed))))
        table.add_row("Total Files", str(self.total_files))
        table.add_row("Completed", str(self.completed))
        table.add_row("Failed", f"[red]{str(self.failed)}[/red]")
        
        if self.completed > 0:
            success_rate = (self.completed / (self.completed + self.failed)) * 100
            table.add_row("Success Rate", f"{success_rate:.1f}%")
            
        return table

    def update(self):
        self.layout["header"].update(Panel("📚 Ebook Processing Status", style="bold blue"))
        self.layout["body"].update(self.progress)
        self.layout["footer"].update(self.create_status_table())

class EbookProcessor:
    def __init__(self):
        # Load environment variables
        load_dotenv('.env.id')
        
        # Initialize OpenAI client
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Set local directory path
        self.local_dir = '/Users/ifzat/Downloads/Ebook/Ebook/compressed'
        
        # Validate AWS credentials
        self._init_aws_clients()

    def _init_aws_clients(self):
        """Initialize and validate AWS clients"""
        try:
            # Check for required environment variables
            required_vars = ['DYNAMODB_ACCESS_KEY_ID', 'DYNAMODB_SECRET_ACCESS_KEY', 'DYNAMODB_REGION', 'DYNAMODB_ID_AND_TAG']
            missing_vars = [var for var in required_vars if not os.getenv(var)]
            
            if missing_vars:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
            # Initialize AWS session with DynamoDB-specific credentials
            self.session = boto3.Session(
                aws_access_key_id=os.getenv('DYNAMODB_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('DYNAMODB_SECRET_ACCESS_KEY'),
                region_name=os.getenv('DYNAMODB_REGION')
            )
            
            # Initialize DynamoDB and validate connection
            self.dynamodb = self.session.resource('dynamodb')
            self.table = self.dynamodb.Table(os.getenv('DYNAMODB_ID_AND_TAG'))
            
            # Test DynamoDB connection
            self.table.get_item(
                Key={'file_key': 'test_connection'}
            )
            
            log_success("AWS credentials validated successfully")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'UnrecognizedClientException':
                raise Exception("Invalid AWS credentials. Please check your DynamoDB access key and secret key.")
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise Exception(f"DynamoDB table {os.getenv('DYNAMODB_ID_AND_TAG')} not found.")
            else:
                raise Exception(f"AWS Error: {str(e)}")
        except NoCredentialsError:
            raise Exception("No AWS credentials found. Please check your DynamoDB environment variables.")
        except Exception as e:
            raise Exception(f"Error initializing AWS clients: {str(e)}")

    def _process_epub(self, file_path):
        """Process EPUB file and return text content"""
        try:
            log_info(f"Opening EPUB file: {file_path}")
            book = epub.read_epub(file_path)
            chapters = []
            is_image_based = True
            
            log_info(f"Processing EPUB items for {os.path.basename(file_path)}")
            for item in book.get_items():
                try:
                    if item.get_type() == ebooklib.ITEM_DOCUMENT:
                        log_info(f"Processing document item: {item.get_name()}")
                        content = item.get_content().decode('utf-8')
                        
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        # Check for images and their alt text
                        images = soup.find_all('img')
                        if images:
                            for img in images:
                                alt_text = img.get('alt', '')
                                if alt_text and not alt_text.endswith(('.jpg', '.jpeg', '.png')):
                                    chapters.append(f"Image description: {alt_text}")
                                    log_info(f"Found image with alt text: {alt_text}")
                                
                                # Get any text in the same paragraph as the image
                                parent = img.find_parent('p')
                                if parent:
                                    text = parent.get_text(strip=True)
                                    if text and not text.endswith(('.jpg', '.jpeg', '.png')):
                                        chapters.append(text)
                                        log_info(f"Found text with image: {text}")
                        
                        # Still try to get any regular text content
                        for tag in soup.find_all(['p', 'div', 'span']):
                            if not tag.find('img'):  # Skip if it contains an image
                                text = tag.get_text(strip=True)
                                if text and len(text) > 10 and not text.endswith(('.jpg', '.jpeg', '.png')):
                                    chapters.append(text)
                                    is_image_based = False
                                    log_info(f"Found text in {tag.name}: {text[:100]}")
                            
                except Exception as item_error:
                    log_error(f"Error processing EPUB item {item.get_name()}: {str(item_error)}")
                    continue
            
            if not chapters:
                if is_image_based:
                    log_warning(f"This appears to be an image-based EPUB with no extractable text")
                    # For image-based EPUBs, we'll use the filename as minimal content
                    base_name = os.path.splitext(os.path.basename(file_path))[0]
                    chapters.append(f"Image-based book: {base_name}")
                else:
                    raise ValueError(f"No text content found in EPUB file: {file_path}")
            
            log_info(f"Successfully extracted {len(chapters)} text elements from {os.path.basename(file_path)}")
            return "\n\n".join(chapters)
            
        except Exception as e:
            log_error(f"Error processing EPUB file {file_path}:")
            log_error(f"Error type: {type(e).__name__}")
            log_error(f"Error message: {str(e)}")
            log_error(f"Stack trace: {traceback.format_exc()}")
            raise

    async def process_single_file(self, file_path, status, task_id):
        """Process a single file through the pipeline"""
        filename = os.path.basename(file_path)
        try:
            log_info(f"Starting to process: {filename}")
            
            # Validate file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Validate file size
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                raise ValueError(f"File is empty: {filename}")
            
            # Check if file is too large (100MB limit)
            if file_size > 100 * 1024 * 1024:
                raise ValueError(f"File too large ({file_size / 1024 / 1024:.2f}MB): {filename}")
            
            # Check for existing record first
            try:
                file_key = f"compressed/{filename}"
                existing_item = self.table.get_item(
                    Key={'file_key': file_key}
                ).get('Item')
                
                if existing_item and existing_item.get('status') == 'active':
                    log_info(f"Skipping {filename} - already processed")
                    status.completed += 1
                    status.update()
                    return {
                        'file_key': file_key,
                        'status': 'skipped',
                        'assistant_id': existing_item.get('assistant_id'),
                        'file_id': existing_item.get('file_id'),
                        'vector_store_id': existing_item.get('vector_store_id')
                    }
            except Exception as e:
                log_warning(f"Error checking DynamoDB for {filename}: {str(e)}")
                # Continue processing even if DynamoDB check fails
            
            # Create a temporary file for EPUB processing if needed
            with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as temp_file:
                try:
                    if filename.lower().endswith('.epub'):
                        log_info(f"Processing EPUB file: {filename}")
                        try:
                            content = self._process_epub(file_path)
                            if not content.strip():
                                raise ValueError("No content extracted from EPUB")
                            temp_file.write(content.encode('utf-8'))
                            temp_file.flush()
                            upload_path = temp_file.name
                            log_success(f"Successfully processed EPUB: {filename}")
                        except Exception as epub_error:
                            log_error(f"EPUB processing error for {filename}:")
                            log_error(f"Error type: {type(epub_error).__name__}")
                            log_error(f"Error message: {str(epub_error)}")
                            log_error(f"Stack trace: {traceback.format_exc()}")
                            raise epub_error
                    else:
                        upload_path = file_path

                    # 1. Upload to OpenAI (20% progress)
                    status.progress.update(task_id, description=f"☁️ Uploading to OpenAI: {filename}")
                    try:
                        with open(upload_path, 'rb') as file:
                            openai_file = self.openai_client.files.create(
                                file=file,
                                purpose='assistants'
                            )
                        log_success(f"Successfully uploaded to OpenAI: {filename}")
                        status.progress.update(task_id, advance=20)
                    except Exception as e:
                        raise Exception(f"OpenAI upload failed: {str(e)}")

                    # Clean up temporary file if it was created
                    if filename.lower().endswith('.epub'):
                        os.unlink(temp_file.name)

                    # 2. Create vector store (20% progress)
                    status.progress.update(task_id, description=f"🗄️ Creating vector store: {filename}")
                    try:
                        vector_store = self.openai_client.beta.vector_stores.create(
                            name=f"Store - {filename}"[:64]
                        )
                        log_success(f"Created vector store for: {filename}")
                        status.progress.update(task_id, advance=20)
                    except Exception as e:
                        raise Exception(f"Vector store creation failed: {str(e)}")

                    # 3. Add file to vector store (20% progress)
                    status.progress.update(task_id, description=f"📥 Adding to vector store: {filename}")
                    try:
                        vector_store_file = self.openai_client.beta.vector_stores.files.create(
                            vector_store_id=vector_store.id,
                            file_id=openai_file.id
                        )
                        log_success(f"Added to vector store: {filename}")
                        status.progress.update(task_id, advance=20)
                    except Exception as e:
                        raise Exception(f"Vector store file creation failed: {str(e)}")

                    # 4. Create assistant (10% progress)
                    status.progress.update(task_id, description=f"🤖 Creating assistant: {filename}")
                    try:
                        assistant = self.openai_client.beta.assistants.create(
                            name=f"Ebook Assistant - {filename}"[:64],
                            instructions="""You are a friendly Malaysian teacher's assistant focusing STRICTLY on helping primary and middle school students understand this specific ebook. Base ALL your answers on the ebook's content.

1. CONTENT ACCURACY:
   - ONLY provide information that exists in this specific ebook
   - Always specify which chapter or section the answer comes from
   - If information isn't in the ebook, say "I can't find this in the ebook" and suggest checking with a teacher
   - For math/science problems, use examples directly from the ebook
   - Never make up or assume information not present in the ebook

2. MULTILINGUAL CONTENT DELIVERY:
   - Respond in these languages based on the student's query:
     * Bahasa Malaysia (BM)
     * English
     * Mandarin (中文)
     * Hindi (हिंदी)
   - When translating ebook content, clearly indicate it's a translation
   - Use exact quotes from the ebook when possible
   - Maintain the original meaning when translating

3. AGE-APPROPRIATE RESPONSES:
   - Use the ebook's own examples and explanations
   - Break down ebook concepts into simpler steps
   - Reference relevant exercises from the ebook
   - Use the same terminology as the ebook
   - Keep explanations at the ebook's intended level

4. EDUCATIONAL GUIDANCE:
   - Guide students to specific pages and sections in the ebook
   - Help with homework using the ebook's methods
   - Explain using the ebook's examples first
   - Follow the ebook's teaching approach
   - Point out key learning points from the ebook

5. VERIFICATION AND CITATION:
   - Always mention which part of the ebook you're referencing
   - If a question goes beyond the ebook's content, clearly say so
   - For step-by-step solutions, follow the ebook's method
   - Link answers to specific exercises or examples in the ebook
   - Encourage students to reference the correct pages

6. ENGAGEMENT WITH EBOOK:
   - Direct students to relevant chapters and sections
   - Use the ebook's illustrations and diagrams when mentioned
   - Reference similar problems from the ebook
   - Help locate specific information within the ebook
   - Show connections between different parts of the ebook

Remember: You are an extension of this ebook. If any question goes beyond the ebook's content, politely explain that you can only answer based on what's in this specific ebook.

Book being referenced: {filename}""",
                            model="gpt-4o-mini",
                            tools=[{"type": "file_search"}],
                            tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}}
                        )
                        log_success(f"Created assistant for: {filename}")
                        status.progress.update(task_id, advance=10)
                    except Exception as e:
                        raise Exception(f"Assistant creation failed: {str(e)}")

                    # 5. Store in DynamoDB (10% progress)
                    status.progress.update(task_id, description=f"💾 Storing data: {filename}")
                    try:
                        item = {
                            'file_key': f"compressed/{filename}",
                            'title': filename,
                            'timestamp': datetime.now().isoformat(),
                            'assistant_id': assistant.id,
                            'file_id': openai_file.id,
                            'vector_store_id': vector_store.id,
                            'status': 'active'
                        }
                        self.table.put_item(Item=item)
                        log_success(f"Stored data for: {filename}")
                        status.progress.update(task_id, advance=10)
                    except Exception as e:
                        raise Exception(f"DynamoDB storage failed: {str(e)}")

                    # Update status
                    status.completed += 1
                    status.update()
                    
                    return {
                        'file_key': f"compressed/{filename}",
                        'status': 'success',
                        'assistant_id': assistant.id,
                        'file_id': openai_file.id,
                        'vector_store_id': vector_store.id
                    }
                    
                except Exception as e:
                    if os.path.exists(temp_file.name):
                        os.unlink(temp_file.name)
                    raise e

        except Exception as e:
            error_details = traceback.format_exc()
            log_error(f"Error processing {filename}:")
            log_error(f"Error type: {type(e).__name__}")
            log_error(f"Error message: {str(e)}")
            log_error(f"Stack trace: {error_details}")
            status.failed += 1
            status.update()
            return {
                'file_key': f"compressed/{filename}",
                'status': 'failed',
                'error': f"{type(e).__name__}: {str(e)}"
            }

    async def process_files(self):
        """Process all PDF and EPUB files in the local directory"""
        status = ProcessingStatus()
        
        try:
            # Get list of PDF and EPUB files
            pdf_files = glob.glob(os.path.join(self.local_dir, '*.pdf'))
            epub_files = glob.glob(os.path.join(self.local_dir, '*.epub'))
            all_files = pdf_files + epub_files
            
            if not all_files:
                log_warning(f"No PDF or EPUB files found in {self.local_dir}")
                return []
                
            log_info(f"Found {len(pdf_files)} PDF files and {len(epub_files)} EPUB files to process")
            status.total_files = len(all_files)
            
            with Live(status.layout, refresh_per_second=4) as live:
                results = []
                processed_files = set()  # Keep track of processed files
                
                # Process all files
                for file_path in all_files:
                    filename = os.path.basename(file_path)
                    if filename in processed_files:
                        continue
                        
                    file_type = "PDF" if filename.lower().endswith('.pdf') else "EPUB"
                    task_id = status.progress.add_task(
                        description=f"Checking {file_type}: {filename}",
                        total=100
                    )
                    
                    # Check for existing record first
                    file_key = f"compressed/{filename}"
                    try:
                        existing_item = self.table.get_item(
                            Key={'file_key': file_key}
                        ).get('Item')
                        
                        if existing_item and existing_item.get('status') == 'active':
                            status.progress.update(task_id, description=f"⏭️ Skipping {file_type} (already exists): {filename}")
                            status.completed += 1
                            status.update()
                            results.append({
                                'file_key': file_key,
                                'status': 'skipped',
                                'assistant_id': existing_item.get('assistant_id'),
                                'file_id': existing_item.get('file_id'),
                                'vector_store_id': existing_item.get('vector_store_id')
                            })
                            processed_files.add(filename)
                            continue
                    except Exception as e:
                        log_warning(f"Error checking DynamoDB for {filename}: {str(e)}")
                    
                    status.progress.update(task_id, description=f"📝 Processing {file_type}: {filename}")
                    result = await self.process_single_file(file_path, status, task_id)
                    results.append(result)
                    processed_files.add(filename)
                    status.update()
                
                # Print final summary
                self._print_rich_summary(results)
                
                return results
                
        except Exception as e:
            error_details = traceback.format_exc()
            log_error("Error in process_files:")
            log_error(f"Error type: {type(e).__name__}")
            log_error(f"Error message: {str(e)}")
            log_error(f"Stack trace: {error_details}")
            raise

    def _print_rich_summary(self, results):
        """Print rich summary table"""
        console.print("\n")
        summary_table = Table(show_header=True, header_style="bold magenta", title="Processing Summary")
        
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Count", justify="right", style="green")
        
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'failed')
        
        summary_table.add_row("Total Processed", str(len(results)))
        summary_table.add_row("Successful", str(successful))
        summary_table.add_row("Failed", f"[red]{failed}[/red]")
        
        console.print(summary_table)
        
        if failed > 0:
            console.print("\n[bold red]Failed Files:[/bold red]")
            failed_table = Table(show_header=True, header_style="bold red")
            failed_table.add_column("File", style="yellow")
            failed_table.add_column("Error", style="red")
            
            for result in results:
                if result['status'] == 'failed':
                    failed_table.add_row(
                        result['file_key'],
                        result.get('error', 'Unknown error')
                    )
            
            console.print(failed_table)

async def main():
    try:
        processor = EbookProcessor()
        await processor.process_files()
    except Exception as e:
        error_details = traceback.format_exc()
        log_error("Critical error:")
        log_error(f"Error type: {type(e).__name__}")
        log_error(f"Error message: {str(e)}")
        log_error(f"Stack trace: {error_details}")
        raise

if __name__ == "__main__":
    asyncio.run(main())