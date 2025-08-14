from pinecone import Pinecone
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Optional
import json
from openai import OpenAI
import os
from dotenv import load_dotenv
from contants import SYSTEM_PROMPT, get_available_books  # Import get_available_books

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PineconeChatbot:
    def __init__(self):
        load_dotenv()
        
        # Initialize OpenAI
        self.openai_client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Get available books
        self.available_books = get_available_books()
        logger.info(f"Available books: {self.available_books}")
        
        # Initialize Pinecone with environment variables
        pinecone_key = os.getenv("PINECONE_API_KEY")
        pinecone_env = os.getenv("PINECONE_ENVIRONMENT")
        if not pinecone_key or not pinecone_env:
            raise ValueError("PINECONE_API_KEY or PINECONE_ENVIRONMENT not found in environment variables")
            
        self.pc = Pinecone(
            api_key=pinecone_key,
            environment=pinecone_env
        )
        
        # Configure index using environment variables
        self.index_name = os.getenv("PINECONE_INDEX", "ebook-store")
        self.namespace = os.getenv("PINECONE_NAMESPACE", "ebooks-store-b7a7f3f3")
        
        try:
            self.index = self.pc.Index(self.index_name)
            logger.info(f"Successfully connected to Pinecone index: {self.index_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Pinecone: {str(e)}")
            raise
        
        # Track conversation context
        self.conversation_history = []

    def get_embedding(self, text: str) -> List[float]:
        """Get embedding for text using OpenAI"""
        try:
            response = self.openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=text,
                encoding_format="float"
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error getting embedding: {str(e)}")
            return None

    def search_relevant_content(self, query: str) -> List[Dict]:
        """Search for relevant content using Pinecone"""
        try:
            # Get embedding for the query
            response = self.openai_client.embeddings.create(
                model="text-embedding-ada-002",
                input=query
            )
            xq = response.data[0].embedding

            # Search in Pinecone
            results = self.index.query(
                vector=xq,
                top_k=10,
                namespace=self.namespace,
                include_metadata=True
            )

            # Extract and process results
            relevant_content = []
            for match in results.matches:
                if match.score < 0.7:  # Adjust threshold as needed
                    continue
                    
                metadata = match.metadata
                relevant_content.append({
                    'file_id': metadata.get('file_id', 'Unknown'),
                    'page': metadata.get('page', 'Unknown'),
                    'text': metadata.get('text', ''),
                    'score': match.score
                })

            # If searching for books, also check titles directly
            if 'book' in query.lower():
                topic = query.lower().replace('book', '').replace('about', '').strip()
                matching_books = [
                    book for book in self.available_books 
                    if topic in book.lower()
                ]
                if matching_books:
                    relevant_content.append({
                        'file_id': 'Book List',
                        'page': '1',
                        'text': f"Found these relevant books: {', '.join(matching_books)}",
                        'score': 1.0
                    })

            return relevant_content

        except Exception as e:
            logger.error(f"Error searching content: {str(e)}")
            return []

    def generate_response(self, query, relevant_content):
        """Generate a response using OpenAI's chat completion."""
        try:
            # Create a more informative system prompt
            system_prompt = f"""You are a helpful educational assistant. Your task is to:
1. Answer questions about the available books in our database
2. Provide detailed information about book content when available
3. Make relevant book recommendations based on user interests
4. If no relevant books are found, politely explain and suggest alternatives

Available books: {self.available_books}

When suggesting books:
- Provide brief descriptions if available
- Group books by categories/themes when relevant
- Explain why you're recommending each book
- If the query is too broad, ask for clarification

Remember to be helpful and educational in your responses."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ]

            if relevant_content:
                context = "Here is some relevant information from our books:\n"
                for content in relevant_content[:3]:  # Limit to top 3 most relevant results
                    context += f"\nFrom {content['file_id']}:\n{content['text']}\n"
                messages.append({"role": "assistant", "content": context})

            response = self.openai_client.chat.completions.create(
                model=os.getenv("GPT_MODEL", "gpt-4o-mini"),
                messages=messages,
                temperature=0.7,
                max_tokens=500
            )

            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            return "I apologize, but I encountered an error generating a response. Please try again."

    def refresh_books(self):
        """Refresh the list of available books"""
        try:
            self.available_books = get_available_books()
            logger.info(f"Refreshed book list. Available books: {self.available_books}")
            return True
        except Exception as e:
            logger.error(f"Error refreshing book list: {str(e)}")
            return False

    def chat(self):
        """Interactive chat loop"""
        # Ensure we have the latest book list
        self.refresh_books()
        
        while True:
            query = input("\nYou: ").strip()
            
            if query.lower() in ['quit', 'exit', 'bye']:
                print("Goodbye!")
                break
                
            print("Searching for relevant content...")
            relevant_content = self.search_relevant_content(query)
            
            if not relevant_content:
                print("I couldn't find any relevant information in the available books to answer your question.")
                continue
                
            print("Generating response...")
            response = self.generate_response(query, relevant_content)
            print(f"\nA: {response}")
                
            # Optional - show references
            show_refs = input("\nWould you like to see the references? (y/n): ").lower()
            if show_refs == 'y':
                print("\nReferences:")
                for i, content in enumerate(relevant_content, 1):
                    print(f"\n{i}. File: {content['file_id']}, Page: {content['page']}")
                    print(f"Relevance score: {content['score']:.2f}")
                    print(f"Content preview: {content['text'][:200]}...")

    def test_connection(self):
        """Test the Pinecone connection"""
        try:
            # Try to describe the index
            description = self.index.describe_index_stats()
            logger.info(f"Successfully connected to Pinecone. Index stats: {description}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Pinecone: {str(e)}")
            return False

def main():
    try:
        # Debug: Print environment variables (remove in production)
        print("\nChecking environment variables:")
        print(f"PINECONE_API_KEY set: {'PINECONE_API_KEY' in os.environ}")
        print(f"PINECONE_ENVIRONMENT set: {'PINECONE_ENVIRONMENT' in os.environ}")
        print(f"PINECONE_INDEX set: {'PINECONE_INDEX' in os.environ}")
        print(f"PINECONE_NAMESPACE set: {'PINECONE_NAMESPACE' in os.environ}")
        print(f"OPENAI_API_KEY set: {'OPENAI_API_KEY' in os.environ}")
        print(f"\nPinecone Environment: {os.getenv('PINECONE_ENVIRONMENT')}")
        print(f"Pinecone Index: {os.getenv('PINECONE_INDEX')}")
        print(f"Pinecone Namespace: {os.getenv('PINECONE_NAMESPACE')}\n")
        
        chatbot = PineconeChatbot()
        if not chatbot.test_connection():
            logger.error("Failed to establish Pinecone connection. Please check your credentials.")
            exit(1)
            
        print("\nWelcome to the Educational Books Chatbot!")
        print("You can ask questions about the content in the database.")
        print("Type 'quit' to exit.\n")
        chatbot.chat()
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()