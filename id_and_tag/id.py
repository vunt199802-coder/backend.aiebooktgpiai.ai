import os
import boto3
from botocore.config import Config
from openai import OpenAI
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TaskProgressColumn
)
import asyncio
import time

# Initialize console for nice output
console = Console()
def log_info(msg): console.print(f"[blue]ℹ️ {msg}[/blue]")
def log_success(msg): console.print(f"[green]✅ {msg}[/green]")
def log_error(msg): console.print(f"[red]❌ {msg}[/red]")

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
        
        # Initialize AWS clients
        self._init_aws_clients()
        
        # Initialize OpenAI client
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

    def _init_aws_clients(self):
        """Initialize AWS clients"""
        log_info("Initializing AWS clients...")
        
        self.session = boto3.Session(
            aws_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY'),
            region_name=os.getenv('S3_REGION')
        )
        
        config = Config(retries=dict(max_attempts=3))
        self.s3_client = self.session.client('s3', config=config)
        self.dynamodb = self.session.resource('dynamodb')
        self.table = self.dynamodb.Table(os.getenv('DYNAMODB_ID_AND_TAG'))
        
        self.bucket = os.getenv('S3_BUCKET')
        self.prefix = os.getenv('S3_COMPRESSED_PREFIX')
        log_success("AWS clients initialized successfully")

    async def process_single_file(self, file_key, status, task_id):
        """Process a single file through the pipeline"""
        try:
            # Check for existing record first
            existing_item = self.table.get_item(
                Key={'file_key': file_key}
            ).get('Item')
            
            if existing_item and existing_item.get('status') == 'active':
                log_info(f"Skipping {file_key} - already processed")
                status.completed += 1
                status.update()
                return {
                    'file_key': file_key,
                    'status': 'skipped',
                    'assistant_id': existing_item.get('assistant_id'),
                    'file_id': existing_item.get('file_id'),
                    'vector_store_id': existing_item.get('vector_store_id')
                }

            # 1. Download file from S3 (20% progress)
            status.progress.update(task_id, description=f"📄 Downloading {os.path.basename(file_key)}")
            local_path = f"/tmp/{os.path.basename(file_key)}"
            self.s3_client.download_file(self.bucket, file_key, local_path)
            status.progress.update(task_id, advance=20)
            
            # 2. Upload to OpenAI (20% progress)
            status.progress.update(task_id, description=f"☁️ Uploading to OpenAI: {os.path.basename(file_key)}")
            with open(local_path, 'rb') as file:
                openai_file = self.openai_client.files.create(
                    file=file,
                    purpose='assistants'
                )
            os.remove(local_path)
            status.progress.update(task_id, advance=20)
            
            # 3. Create vector store (20% progress)
            status.progress.update(task_id, description=f"🗄️ Creating vector store: {os.path.basename(file_key)}")
            file_name = os.path.basename(file_key)
            vector_store = self.openai_client.beta.vector_stores.create(
                name=f"Store - {file_name}"[:64]
            )
            status.progress.update(task_id, advance=20)
            
            # 4. Add file to vector store (20% progress)
            status.progress.update(task_id, description=f"📥 Adding to vector store: {os.path.basename(file_key)}")
            vector_store_file = self.openai_client.beta.vector_stores.files.create(
                vector_store_id=vector_store.id,
                file_id=openai_file.id
            )
            status.progress.update(task_id, advance=20)
            
            # 5. Create assistant (10% progress)
            status.progress.update(task_id, description=f"🤖 Creating assistant: {os.path.basename(file_key)}")
            assistant = self.openai_client.beta.assistants.create(
                name=f"Ebook Assistant - {file_name}"[:64],
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

Book being referenced: {file_name}""",
                model="gpt-4o-mini",
                tools=[{"type": "file_search"}],
                tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}}
            )
            status.progress.update(task_id, advance=10)
            
            # 6. Store in DynamoDB (10% progress)
            status.progress.update(task_id, description=f"💾 Storing data: {os.path.basename(file_key)}")
            item = {
                'file_key': f"compressed/{file_key}",
                'title': file_name,
                'timestamp': datetime.now().isoformat(),
                'assistant_id': assistant.id,
                'file_id': openai_file.id,
                'vector_store_id': vector_store.id,
                'status': 'active'
            }
            self.table.put_item(Item=item)
            status.progress.update(task_id, advance=10)
            
            # Update status
            status.completed += 1
            status.update()
            
            return {
                'file_key': file_key,
                'status': 'success',
                'assistant_id': assistant.id,
                'file_id': openai_file.id,
                'vector_store_id': vector_store.id
            }
            
        except Exception as e:
            log_error(f"Error processing {file_key}: {e}")
            status.failed += 1
            status.update()
            return {
                'file_key': file_key,
                'status': 'failed',
                'error': str(e)
            }

    async def process_files(self):
        """Process all files in the S3 prefix"""
        try:
            # Initialize status display
            status = ProcessingStatus()
            
            # List files in S3
            log_info("Listing files from S3...")
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix
            )
            
            if 'Contents' not in response:
                log_info("No files found")
                return []
                
            # Get PDF files
            pdf_files = [
                f"compressed/{os.path.basename(item['Key'])}" for item in response['Contents']
                if item['Key'].lower().endswith('.pdf')
            ]
            
            status.total_files = len(pdf_files)
            log_info(f"Found {status.total_files} PDF files")
            
            # Process files with progress tracking
            results = []
            with Live(status.layout, refresh_per_second=4) as live:
                for file_key in pdf_files:
                    task_id = status.progress.add_task(
                        description=f"Starting {os.path.basename(file_key)}",
                        total=100
                    )
                    
                    result = await self.process_single_file(file_key, status, task_id)
                    results.append(result)
                    status.update()
            
            # Print final summary
            self._print_rich_summary(results)
            return results
            
        except Exception as e:
            log_error(f"Error in process_files: {e}")
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
        log_error(f"Critical error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())