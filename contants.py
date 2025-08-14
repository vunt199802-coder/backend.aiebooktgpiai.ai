# constants.py

import boto3
import os
from typing import List, Dict
from pinecone import Pinecone
from openai import OpenAI
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_dynamodb_books() -> List[Dict]:
    """Get book information from DynamoDB"""
    try:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        
        table = dynamodb.Table('ebook-store')
        response = table.scan()
        books = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            books.extend(response.get('Items', []))
        
        return books
    except Exception as e:
        logger.error(f"Error getting books from DynamoDB: {e}")
        return []

def get_s3_books() -> List[str]:
    """Get list of available books from S3"""
    try:
        s3_client = boto3.client(
            's3',
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        
        response = s3_client.list_objects_v2(
            Bucket=os.getenv("S3_BUCKET", "primary-school-ebook-data"),
            Prefix='compressed/'
        )
        
        available_books = []
        if 'Contents' in response:
            for obj in response['Contents']:
                filename = os.path.splitext(os.path.basename(obj['Key']))[0]
                if filename:
                    available_books.append(filename)
        
        return available_books
    except Exception as e:
        logger.error(f"Error getting books from S3: {e}")
        return []

def init_pinecone():
    """Initialize Pinecone connection"""
    try:
        pinecone_key = os.getenv("PINECONE_API_KEY")
        pinecone_env = os.getenv("PINECONE_ENVIRONMENT")
        
        if not pinecone_key or not pinecone_env:
            raise ValueError("PINECONE_API_KEY or PINECONE_ENVIRONMENT not found in environment variables")
        
        pc = Pinecone(
            api_key=pinecone_key,
            environment=pinecone_env
        )
        
        index_name = os.getenv("PINECONE_INDEX", "ebook-store")
        namespace = os.getenv("PINECONE_NAMESPACE", "ebooks-store-b7a7f3f3")
        
        try:
            index = pc.Index(index_name)
            # Verify index is accessible
            stats = index.describe_index_stats()
            logger.info(f"Connected to Pinecone index: {index_name}")
            return index, namespace
        except Exception as e:
            logger.error(f"Failed to connect to Pinecone index: {e}")
            return None, None
            
    except Exception as e:
        logger.error(f"Error initializing Pinecone: {e}")
        return None, None

def get_openai_client():
    """Get OpenAI client"""
    try:
        return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    except Exception as e:
        logger.error(f"Error initializing OpenAI client: {e}")
        return None

# Initialize services
PINECONE_INDEX, PINECONE_NAMESPACE = init_pinecone()
OPENAI_CLIENT = get_openai_client()

# Get book information from different sources
DYNAMO_BOOKS = get_dynamodb_books()
S3_BOOKS = get_s3_books()  # Keep as list for title matching

# Create a set of all available books
ALL_BOOKS = set()

# Add S3 books first (they are physically available)
ALL_BOOKS.update(S3_BOOKS)

# Add DynamoDB books if they exist
if DYNAMO_BOOKS:
    ALL_BOOKS.update(book.get('title', '') for book in DYNAMO_BOOKS if book.get('title'))

# Convert back to sorted list for consistency with chatbot.py
AVAILABLE_BOOKS = sorted(ALL_BOOKS)
BOOKS_LIST = ", ".join(AVAILABLE_BOOKS) if AVAILABLE_BOOKS else "No books currently available"

SYSTEM_PROMPT = f'''You are an intelligent Malaysian educational AI assistant specializing in helping students understand specific ebooks.

Available Books: {BOOKS_LIST}

1. CONTENT & SEARCH CAPABILITIES:
   - Use Pinecone's semantic search to find relevant content
   - Search across all available books
   - Consider both exact and semantic matches
   - Check book titles directly for topic matches
   - Use OpenAI embeddings for semantic understanding

2. BOOK ACCESS & AVAILABILITY:
   - Check book availability in S3
   - Verify metadata in DynamoDB
   - When a book title is available in S3:
     * Encourage the user to start reading
   - When a book is mentioned in DynamoDB:
     * Verify it exists in S3 before suggesting
   - Consider content indexed in Pinecone
   - Provide accurate book information
   - Indicate if a book is currently accessible

3. MULTILINGUAL SUPPORT:
   - Provide support in:
     * Bahasa Malaysia (BM)
     * English
     * Mandarin (中文)
     * Tamil (தமிழ்)
   - Use appropriate language for responses
   - Maintain accuracy across languages

4. EDUCATIONAL GUIDANCE:
   - Reference specific book content
   - Provide accurate citations
   - Use book-specific examples
   - Support curriculum alignment
   - Guide through book content

CRITICAL NOTES:
1. Book Search Strategy:
   - First check direct title matches
   - Then use semantic search
   - Consider book series (e.g., ALAM HAIWAN for animals)
   - Match keywords in multiple languages
   - Include related books when relevant
   - Direct users to start reading immediately
   - Mention if additional books are available on the topic
   - Suggest related books when appropriate
   - Help users find the right book

2. Content Verification:
   - Check book availability
   - Verify content accessibility
   - Use semantic search for details
   - Consider multiple sources
   - Provide accurate references

3. Response Quality:
   - Base answers on available content
   - Include specific book titles
   - Group related books together
   - Maintain educational context
   - Consider user's language preference'''