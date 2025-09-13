# constants.py

import os
from typing import List, Dict
from pinecone import Pinecone
from openai import OpenAI
import logging
from database.connection import SessionLocal
from database.models import Books

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_neon_books() -> List[Dict]:
    """Get book information from Neon DB"""
    try:
        db = SessionLocal()
        try:
            books = db.query(Books).filter(Books.status == 'active').all()
            books_data = []
            for book in books:
                books_data.append({
                    'id': str(book.id),
                    'title': book.title,
                    'file_key': book.file_key,
                    'url': book.url,
                    'thumb_url': book.thumb_url,
                    'thumbnail': book.thumbnail,
                    'assistant_id': book.assistant_id,
                    'file_id': book.file_id,
                    'vector_store_id': book.vector_store_id,
                    'language': book.language,
                    'genres': book.genres or [],
                    'author': book.author,
                    'pages': book.pages,
                    'status': book.status,
                    'created_at': book.created_at.isoformat() if book.created_at else None,
                    'updated_at': book.updated_at.isoformat() if book.updated_at else None
                })
            return books_data
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error getting books from Neon DB: {e}")
        return []

def get_available_book_titles() -> List[str]:
    """Get list of available book titles from Neon DB"""
    try:
        db = SessionLocal()
        try:
            books = db.query(Books).filter(Books.status == 'indexed').all()
            return [book.title for book in books if book.title]
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error getting book titles from Neon DB: {e}")
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

# Get book information from Neon DB
NEON_BOOKS = get_neon_books()
AVAILABLE_BOOKS = get_available_book_titles()
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
   - Check book availability in Neon DB
   - Verify book metadata and status
   - When a book title is available in the database:
     * Encourage the user to start reading
     * Provide book details (author, pages, language, genres)
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
   - Check book availability in Neon DB
   - Verify content accessibility
   - Use semantic search for details
   - Consider database metadata
   - Provide accurate references

3. Response Quality:
   - Base answers on available content
   - Include specific book titles
   - Group related books together
   - Maintain educational context
   - Consider user's language preference'''