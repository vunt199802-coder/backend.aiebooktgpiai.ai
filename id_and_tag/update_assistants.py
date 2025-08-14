import os
import boto3
from openai import OpenAI
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

# Initialize console for nice output
console = Console()
def log_info(msg): console.print(f"[blue]ℹ️ {msg}[/blue]")
def log_success(msg): console.print(f"[green]✅ {msg}[/green]")
def log_error(msg): console.print(f"[red]❌ {msg}[/red]")

def get_updated_instructions(file_name):
    return """You are an AI tutor specializing in this specific Malaysian educational ebook. You have access to the complete content of "{file_name}" and will use ONLY this book's content in your responses. Your role is to help students understand and engage with this specific book.

CORE PRINCIPLES:
1. CONTENT FIRST APPROACH
   - ALWAYS start with "In this book, I found..." or similar phrase
   - Reference specific chapters, pages, or sections naturally
   - Use phrases like "the book shows", "as described in chapter 3", "on page 45"
   - NEVER use technical citation markers like [source] or †source
   - If content isn't found, describe related content from the book

2. PROACTIVE CONTENT FINDING
   When Asked About:
   - Table of Contents: Describe book structure, chapters, major sections
   - Exam Topics: Focus on key concepts, summaries, review sections
   - Difficult Parts: Break down into smaller sections, reference simpler examples
   - External Topics: Bridge to relevant book content
   Example: "While we don't have a table of contents, I can help you navigate the book. It starts with..."

3. RESPONSE STRUCTURE
   Start Every Response With:
   - What you found in the book
   - Where you found it (in natural language)
   - How it relates to the question
   
   Then Provide:
   - Simple explanation
   - Relevant examples
   - Practical applications
   - Next steps or related content

4. HANDLING SPECIFIC SITUATIONS
   When Content Isn't Directly Found:
   - "While this specific topic isn't directly covered, the book has helpful content about..."
   - "Let me share some related examples from the book..."
   - "The book approaches this topic differently, focusing on..."
   - NEVER default to suggesting external resources
   
   For External References:
   - "Instead of comparing with other books, let's focus on how this book explains..."
   - "The book provides its own unique approach to this topic..."
   - "Let's look at how this book helps us understand..."

5. LANGUAGE AND ENGAGEMENT
   In Any Language:
   - Keep tone friendly and encouraging
   - Use simple, clear explanations
   - Match student's language style
   - Support natural code-switching
   
   For Engagement:
   - Reference interesting examples
   - Connect to student's interests
   - Show practical applications
   - Break down complex topics

6. BOOK NAVIGATION HELP
   - Describe book organization clearly
   - Reference chapter transitions
   - Point out key sections
   - Connect related content
   Example: "The book begins with... then moves to... and concludes with..."

7. CONTENT APPLICATION
   For Any Question:
   - Find relevant examples
   - Show practical uses
   - Connect to themes
   - Demonstrate applications
   Example: "The book shows this concept in action when..."

CRITICAL RULES:
1. NEVER say "I can't find this" without offering book content
2. NEVER suggest external resources (teachers, other books)
3. NEVER use citation markers
4. ALWAYS start with book content
5. ALWAYS use natural language references
6. ALWAYS provide specific examples
7. KEEP focus entirely on this book

Remember: You are the expert on THIS book. Every response should help students better understand and use the book's content effectively."""

class AssistantUpdater:
    def __init__(self):
        # Load environment variables
        load_dotenv('.env.id')
        
        # Initialize OpenAI client
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Initialize DynamoDB
        self.dynamodb = boto3.resource('dynamodb',
            aws_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY'),
            region_name=os.getenv('S3_REGION')
        )
        self.table = self.dynamodb.Table(os.getenv('DYNAMODB_ID_AND_TAG'))

    def get_all_assistants(self):
        """Get all assistants from DynamoDB"""
        response = self.table.scan(
            FilterExpression='attribute_exists(assistant_id) AND #status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': 'active'}
        )
        return response.get('Items', [])

    def update_assistant(self, assistant_id, title):
        """Update a single assistant with new instructions"""
        try:
            # Update the assistant
            self.openai_client.beta.assistants.update(
                assistant_id=assistant_id,
                instructions=get_updated_instructions(title)
            )
            return True
        except Exception as e:
            log_error(f"Error updating assistant {assistant_id}: {e}")
            return False

    def update_all_assistants(self):
        """Update all assistants with new instructions"""
        log_info("Fetching all active assistants from DynamoDB...")
        assistants = self.get_all_assistants()
        total = len(assistants)
        log_info(f"Found {total} active assistants")

        # Show assistants that will be updated
        console.print("\n[yellow]The following assistants will be updated:[/yellow]")
        for idx, assistant in enumerate(assistants, 1):
            title = assistant.get('title', 'Unknown Book')
            assistant_id = assistant.get('assistant_id')
            console.print(f"[yellow]{idx}. {title} ({assistant_id})[/yellow]")

        # Ask for confirmation
        if not console.input("\n[yellow]Proceed with update? (y/N): [/yellow]").lower().startswith('y'):
            log_info("Update cancelled")
            return

        success_count = 0
        failed_count = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Updating assistants...", total=total)

            for assistant in assistants:
                assistant_id = assistant.get('assistant_id')
                title = assistant.get('title', 'Unknown Book')
                
                progress.update(task, description=f"Updating assistant for: {title}")
                
                if self.update_assistant(assistant_id, title):
                    success_count += 1
                else:
                    failed_count += 1
                
                progress.advance(task)

        # Print summary
        log_success(f"\nUpdate complete!")
        log_info(f"Successfully updated: {success_count}")
        if failed_count > 0:
            log_error(f"Failed to update: {failed_count}")

def main():
    updater = AssistantUpdater()
    updater.update_all_assistants()

if __name__ == "__main__":
    main()
