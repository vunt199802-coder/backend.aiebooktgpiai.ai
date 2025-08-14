import os
from openai import OpenAI
from dotenv import load_dotenv
from rich.console import Console
from rich import print as rprint
import time
import boto3
import random

# Initialize console for nice output
console = Console()

class PromptTester:
    def __init__(self):
        # Load environment variables
        load_dotenv('.env.id')
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Initialize DynamoDB
        self.dynamodb = boto3.resource('dynamodb',
            aws_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY'),
            region_name=os.getenv('S3_REGION')
        )
        self.table = self.dynamodb.Table(os.getenv('DYNAMODB_ID_AND_TAG'))

    def get_random_assistant(self):
        """Get a random active assistant from DynamoDB"""
        response = self.table.scan(
            FilterExpression='attribute_exists(assistant_id) AND #status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': 'active'}
        )
        
        items = response.get('Items', [])
        if not items:
            raise Exception("No active assistants found in DynamoDB")
            
        # Randomly select one assistant
        assistant = random.choice(items)
        console.print(f"\n[bold blue]Using book:[/bold blue] {assistant.get('title', 'Unknown')}")
        console.print(f"[bold blue]Assistant ID:[/bold blue] {assistant.get('assistant_id', 'Unknown')}\n")
        
        return assistant.get('assistant_id'), assistant.get('title', 'Unknown')

    def create_test_assistant(self):
        """Create a test assistant with the new prompt"""
        instructions = """You are an AI tutor specializing in this specific Malaysian educational ebook. You have access to the complete content of "{file_name}" and will use ONLY this book's content in your responses. Your role is to help students understand and engage with this specific book.

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

        # Create test assistant
        assistant = self.client.beta.assistants.create(
            name="Test Assistant - Prompt Evaluation",
            instructions=instructions,
            model="gpt-4o-mini",  # Using specified model
            tools=[{"type": "file_search"}]
        )
        return assistant

    def create_thread(self):
        """Create a new conversation thread"""
        return self.client.beta.threads.create()

    def get_response(self, thread_id, assistant_id, message):
        """Send a message and get the response"""
        # Add the message to the thread
        self.client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=message
        )

        # Run the assistant
        run = self.client.beta.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id
        )

        # Wait for the response
        while True:
            run = self.client.beta.threads.runs.retrieve(
                thread_id=thread_id,
                run_id=run.id
            )
            if run.status == 'completed':
                break
            time.sleep(1)

        # Get the messages
        messages = self.client.beta.threads.messages.list(
            thread_id=thread_id
        )
        
        # Return the latest assistant message
        for msg in messages.data:
            if msg.role == "assistant":
                return msg.content[0].text.value
        return None

    def run_test_scenarios(self):
        """Run through various test scenarios"""
        console.print("\n[bold blue]Getting random assistant from DynamoDB...[/bold blue]")
        assistant_id, book_title = self.get_random_assistant()
        thread = self.create_thread()

        # Define all possible scenarios
        all_scenarios = [
            {
                "name": "Basic Reading Comprehension",
                "message": "Saya tak faham bab 3. Boleh tolong terangkan dengan cara yang lebih mudah?",
                "expected": "Should provide simple explanation in BM with specific chapter references"
            },
            {
                "name": "Homework Help",
                "message": "I need help with the exercises on page 45. Can you guide me step by step?",
                "expected": "Should provide structured guidance using book's methodology"
            },
            {
                "name": "Multilingual Support",
                "message": "我不明白这一课。请用华语解释一下。",
                "expected": "Should respond in Chinese with clear explanations"
            },
            {
                "name": "Exam Preparation",
                "message": "How can I prepare for my PT3 using this chapter?",
                "expected": "Should link content to PT3 format and provide study strategy"
            },
            {
                "name": "Interactive Learning",
                "message": "This math problem is too hard. Can you break it down for me?",
                "expected": "Should provide scaffolded explanation with examples from book"
            },
            {
                "name": "Emotional Support",
                "message": "I'm feeling very stressed about my upcoming exam. I don't think I can remember everything in this chapter.",
                "expected": "Should show empathy while providing practical study advice from the book"
            },
            {
                "name": "Cultural Context",
                "message": "Can you explain this using something similar to Hari Raya celebration as an example?",
                "expected": "Should incorporate Malaysian cultural reference while staying within book content"
            },
            {
                "name": "Age-Appropriate Response",
                "message": "Cikgu kata saya kena buat latihan ni tapi saya main game je. Macam mana nak focus?",
                "expected": "Should provide friendly, age-appropriate motivation and practical tips"
            },
            {
                "name": "Casual Conversation",
                "message": "eh bestnya buku ni. tapi susah sikit bahagian tengah tu",
                "expected": "Should maintain casual tone while steering back to educational content"
            },
            {
                "name": "Mixed Language",
                "message": "Can explain tak chapter 4? I try to faham but very confusing lah",
                "expected": "Should handle Manglish naturally while providing clear explanation"
            },
            {
                "name": "Local Context",
                "message": "Macam mana nak relate topic ni dengan kedai mamak dekat rumah saya?",
                "expected": "Should use local context while maintaining educational focus"
            },
            {
                "name": "Student Frustration",
                "message": "Dah 3 kali baca still tak faham. Boring la book ni! 😫",
                "expected": "Should handle frustration empathetically while making content engaging"
            },
            {
                "name": "Peer Learning",
                "message": "My friend kata different answer. Who is correct?",
                "expected": "Should handle disagreements diplomatically while citing book content"
            },
            {
                "name": "Time Management",
                "message": "Esok exam dah! Macam mana nak study semua ni?",
                "expected": "Should provide calm, practical guidance for last-minute preparation"
            },
            {
                "name": "Content Boundaries Test",
                "message": "Eh you tau tak pasal BTS? Boleh relate dengan topic ni?",
                "expected": "Should politely redirect to book content without using external references"
            },
            {
                "name": "Book Navigation",
                "message": "Ada table of contents tak? Nak tau chapter mana yang penting untuk exam",
                "expected": "Should help navigate book structure and highlight key chapters"
            },
            {
                "name": "Subject Connection",
                "message": "How does this connect with what we learned in other subjects?",
                "expected": "Should focus on book content while acknowledging curriculum connections"
            },
            {
                "name": "Visual Learning",
                "message": "Ada gambar rajah tak dalam buku ni? Saya lebih suka belajar guna visual",
                "expected": "Should reference book's visual elements if available"
            },
            {
                "name": "Practical Application",
                "message": "When will I ever use this in real life?",
                "expected": "Should relate book content to practical Malaysian context"
            },
            {
                "name": "Learning Style",
                "message": "I'm more of a hands-on learner. How should I study this book?",
                "expected": "Should suggest learning strategies based on book's content"
            },
            {
                "name": "Digital Features",
                "message": "Got any interactive parts in this ebook?",
                "expected": "Should explain available digital features if any"
            },
            {
                "name": "Cross-Book Reference",
                "message": "My other textbook explains this differently. Which one should I follow?",
                "expected": "Should diplomatically focus on current book's explanation"
            },
            {
                "name": "Study Group",
                "message": "How can I use this book for group study with my friends?",
                "expected": "Should suggest collaborative learning based on book content"
            },
            {
                "name": "Exam Focus",
                "message": "Which parts of this book usually come out in exam?",
                "expected": "Should guide based on book's emphasis and curriculum alignment"
            },
            {
                "name": "Book Difficulty",
                "message": "This book seems too advanced. Can start from where?",
                "expected": "Should suggest appropriate entry points in the book"
            },
            {
                "name": "Character Analysis",
                "message": "Boleh cerita pasal watak utama dalam buku ni?",
                "expected": "Should describe main characters using book's content"
            },
            {
                "name": "Plot Understanding",
                "message": "What's the main story about? I'm a bit confused.",
                "expected": "Should summarize main plot points from the book"
            },
            {
                "name": "Theme Discussion",
                "message": "Apa tema utama dalam buku ni? Kenapa penting?",
                "expected": "Should explain main themes using book examples"
            },
            {
                "name": "Setting Description",
                "message": "When and where does this story take place?",
                "expected": "Should describe setting using book details"
            },
            {
                "name": "Vocabulary Help",
                "message": "Ada banyak perkataan susah. Macam mana nak ingat semua?",
                "expected": "Should help with vocabulary using book context"
            },
            {
                "name": "Story Elements",
                "message": "What's the climax of the story? How does it end?",
                "expected": "Should explain story structure using book content"
            },
            {
                "name": "Character Development",
                "message": "How does the main character change throughout the story?",
                "expected": "Should trace character development using book examples"
            },
            {
                "name": "Moral Values",
                "message": "Apa nilai moral yang boleh kita belajar dari cerita ni?",
                "expected": "Should discuss moral lessons from the book"
            },
            {
                "name": "Literary Devices",
                "message": "Can you explain the metaphors used in Chapter 2?",
                "expected": "Should explain literary devices using book examples"
            },
            {
                "name": "Story Timeline",
                "message": "Boleh explain timeline cerita ni? Saya confuse sikit.",
                "expected": "Should clarify story sequence using book events"
            },
            {
                "name": "Character Relationships",
                "message": "How are the characters connected to each other?",
                "expected": "Should explain character relationships from book"
            },
            {
                "name": "Story Context",
                "message": "Why is this story important for Malaysian students?",
                "expected": "Should relate story relevance using book content"
            },
            {
                "name": "Plot Twist",
                "message": "I don't understand why this happened in Chapter 4",
                "expected": "Should explain plot developments using book content"
            },
            {
                "name": "Story Background",
                "message": "What's the historical background of this story?",
                "expected": "Should explain historical context from book"
            },
            {
                "name": "Character Motivation",
                "message": "Why did the main character make that decision?",
                "expected": "Should explain character motivations using book"
            }
        ]

        # Randomly select 10 scenarios to test
        test_scenarios = random.sample(all_scenarios, min(10, len(all_scenarios)))
        
        console.print(f"\n[bold yellow]Selected {len(test_scenarios)} random scenarios for testing[/bold yellow]")

        for i, scenario in enumerate(test_scenarios, 1):
            console.print(f"\n[bold magenta]Test {i} of {len(test_scenarios)}[/bold magenta]")
            console.print(f"[bold yellow]Scenario: {scenario['name']}[/bold yellow]")
            console.print(f"[cyan]Message:[/cyan] {scenario['message']}")
            console.print(f"[cyan]Expected:[/cyan] {scenario['expected']}")
            
            response = self.get_response(thread.id, assistant_id, scenario['message'])
            console.print("\n[green]Response:[/green]")
            console.print(response)
            
            # Ask for evaluation
            console.print("\n[bold]Rate this response (1-5):[/bold]")
            rating = input()
            console.print("[bold]Comments (optional):[/bold]")
            comments = input()
            
            # Save results (optional)
            self.save_test_result(scenario['name'], rating, comments, book_title)
            
            console.print("\n[bold cyan]Press Enter to continue to next test...[/bold cyan]")
            input()

        self.print_test_summary()
        console.print("\n[bold green]Testing completed![/bold green]")

    def save_test_result(self, scenario_name, rating, comments, book_title):
        """Save test results for analysis"""
        # You can implement saving to a file or database here
        pass

    def print_test_summary(self):
        """Print summary of all test results"""
        # You can implement summary statistics here
        pass

def main():
    tester = PromptTester()
    tester.run_test_scenarios()

if __name__ == "__main__":
    main()
