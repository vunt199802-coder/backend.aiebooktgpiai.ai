import boto3
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Literal
import sys
import os
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
from openai import OpenAI
from pydantic import BaseModel, Field

class BookAnalysis(BaseModel):
    language: Literal['Malay', 'English', 'Mandarin', 'Hindi']
    genres: List[Literal['Comics', 'Storybooks', 'History', 'Moral Education', 
                        'Biographies', 'Popular Science', 'Others']]

    @property
    def valid_genres(self) -> List[str]:
        return ['Comics', 'Storybooks', 'History', 'Moral Education', 
                'Biographies', 'Popular Science', 'Others']

    @property
    def valid_languages(self) -> List[str]:
        return ['Malay', 'English', 'Mandarin', 'Hindi']

class EbookTagger:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('id_and_tag')
        self.console = Console()
        self.client = OpenAI()

        # Progress tracking
        self.total_books = 0
        self.tagged_books = 0
        self.skipped_books = 0
        self.start_time = time.time()
        self.book_times: List[float] = []

    def get_all_books(self) -> List[Dict[str, Any]]:
        """Scan DynamoDB table for all books."""
        try:
            response = self.table.scan()
            self.total_books = response['Count']
            return response['Items']
        except Exception as e:
            self.console.print(f"[red]Error scanning table: {str(e)}")
            sys.exit(1)

    def get_assistant_analysis(self, assistant_id: str, title: str) -> BookAnalysis:
        """Get language and genre analysis from OpenAI Assistant."""
        try:
            self.console.print(f"[cyan]Processing Assistant ID:[/cyan] '{assistant_id}'")
            assistant_id = assistant_id.strip()
            
            # Create thread and messages
            thread = self.client.beta.threads.create()
            
            # Create the system message with strict formatting requirements
            message = self.client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=f"""Analyze the book titled "{title}" and classify it according to these exact categories:

LANGUAGES (choose exactly ONE):
- Malay
- English
- Mandarin
- Hindi

GENRES (choose one or more):
- Comics
- Storybooks
- History
- Moral Education
- Biographies
- Popular Science
- Others

Respond ONLY with a JSON object in this exact format:
{{
    "language": "one_of_the_languages_above",
    "genres": ["one_or_more_genres_from_above"]
}}

Rules:
1. Use ONLY the exact categories listed above
2. Do not create new categories
3. Do not modify category names (e.g., don't shorten "Popular Science" to "Science")
4. Select exactly one language
5. Select at least one genre
6. Return only the JSON object, no additional text"""
            )

            # Run the assistant
            run = self.client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id
            )

            # Wait for completion
            while True:
                run_status = self.client.beta.threads.runs.retrieve(
                    thread_id=thread.id,
                    run_id=run.id
                )
                if run_status.status == 'completed':
                    break
                elif run_status.status in ['failed', 'cancelled', 'expired']:
                    raise Exception(f"Assistant run failed with status: {run_status.status}")
                time.sleep(1)

            # Get the response
            messages = self.client.beta.threads.messages.list(thread_id=thread.id)
            response_text = messages.data[0].content[0].text.value.strip()
            
            # Clean up the response
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()
            
            # Parse with Pydantic model
            self.console.print(f"[green]Raw response:[/green] {response_text}")
            analysis = BookAnalysis.model_validate_json(response_text)
            self.console.print(f"[green]Validated analysis:[/green] {analysis.model_dump_json()}")
            
            return analysis

        except Exception as e:
            self.console.print(f"[red]Error in analysis: {str(e)}")
            # Return default values on error
            return BookAnalysis(language="Others", genres=["Others"])

    def update_book_tags(self, book: Dict[str, Any], analysis: BookAnalysis) -> bool:
        """Update book tags in DynamoDB."""
        try:
            self.table.update_item(
                Key={
                    'file_key': book['file_key']  # Use file_key as the primary key
                },
                UpdateExpression="""
                    SET book_language = :lang,
                        book_genres = :genres,
                        tagging_status = :status,
                        tagging_timestamp = :timestamp
                """,
                ExpressionAttributeValues={
                    ':lang': analysis.language,
                    ':genres': analysis.genres,
                    ':status': 'TAGGED',
                    ':timestamp': datetime.now(timezone.utc).isoformat()
                }
            )
            return True
        except Exception as e:
            self.console.print(f"[red]Error updating book: {str(e)}")
            return False

    def calculate_progress_metrics(self) -> Dict[str, Any]:
        """Calculate progress metrics."""
        elapsed_time = time.time() - self.start_time
        avg_time_per_book = sum(self.book_times) / len(self.book_times) if self.book_times else 0
        remaining_books = self.total_books - (self.tagged_books + self.skipped_books)
        estimated_remaining_time = remaining_books * avg_time_per_book if avg_time_per_book > 0 else 0
        
        return {
            'total_books': self.total_books,
            'tagged_books': self.tagged_books,
            'skipped_books': self.skipped_books,
            'remaining_books': remaining_books,
            'progress_percentage': ((self.tagged_books + self.skipped_books) / self.total_books * 100) if self.total_books > 0 else 0,
            'elapsed_time': elapsed_time,
            'avg_time_per_book': avg_time_per_book,
            'estimated_remaining_time': estimated_remaining_time
        }

    def display_progress(self, metrics: Dict[str, Any]):
        """Display progress metrics."""
        self.console.print("\n[bold green]Progress Report[/bold green]")
        self.console.print(f"Total Books: {metrics['total_books']}")
        self.console.print(f"Tagged in this session: {metrics['tagged_books']}")
        self.console.print(f"Skipped (already tagged): {metrics['skipped_books']}")
        self.console.print(f"Remaining: {metrics['remaining_books']}")
        self.console.print(f"Overall Progress: {metrics['progress_percentage']:.1f}%")
        self.console.print("\n[bold green]Time Metrics[/bold green]")
        self.console.print(f"Session Duration: {metrics['elapsed_time']/60:.1f} mins")
        self.console.print(f"Avg Time per Book: {metrics['avg_time_per_book']/60:.1f} mins")
        self.console.print(f"Est. Time Remaining: {metrics['estimated_remaining_time']/60:.1f} mins")

    def run(self):
        """Main execution flow."""
        self.console.print("[bold]Starting Automated Ebook Tagging System[/bold]")
        
        books = self.get_all_books()
        
        with Progress(SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn()) as progress:
            task = progress.add_task("[cyan]Tagging books...", total=len(books))
            
            for book in books:
                book_start_time = time.time()
                
                if book.get('tagging_status') == 'TAGGED':
                    self.skipped_books += 1
                    self.console.print(f"\n[yellow]Skipping already tagged book: {book.get('title', 'Unknown Title')}[/yellow]")
                    progress.advance(task)
                    continue
                
                self.console.print(f"\n[bold blue]Processing book: {book.get('title', 'Unknown Title')}[/bold blue]")
                
                analysis = self.get_assistant_analysis(book['assistant_id'], book.get('title', ''))
                
                self.console.print(f"Language detected: [yellow]{analysis.language}[/yellow]")
                self.console.print(f"Genres detected: [yellow]{', '.join(analysis.genres)}[/yellow]")
                
                if self.update_book_tags(book, analysis):
                    self.tagged_books += 1
                    book_time = time.time() - book_start_time
                    self.book_times.append(book_time)
                    
                    metrics = self.calculate_progress_metrics()
                    self.display_progress(metrics)
                
                progress.advance(task)

        self.console.print("[bold green]Tagging session completed![/bold green]")

if __name__ == "__main__":
    tagger = EbookTagger()
    tagger.run()