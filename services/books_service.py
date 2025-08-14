from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from database.models import Books
import uuid
from datetime import datetime
from typing import List, Optional


def get_all_books(db: Session, page: int = 1, limit: int = 20, status: Optional[str] = None):
    """Get all books with optional pagination and status filtering"""
    try:
        query = db.query(Books)
        
        if status:
            query = query.filter(Books.status == status)
        
        total_count = query.count()
        
        books = query.offset((page - 1) * limit).limit(limit).all()
        
        return {
            "books": books,
            "total_count": total_count,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        db.rollback()
        return None


def get_book_by_id(book_id: str, db: Session):
    """Get a single book by ID"""
    try:
        return db.query(Books).filter(Books.id == book_id).first()
    except Exception as e:
        db.rollback()
        return None


def add_book(book_data: dict, db: Session):
    """Add a new book"""
    try:
        new_book = Books(
            id=book_data.get("id", uuid.uuid4()),
            title=book_data["title"],
            file_key=book_data.get("file_key", ""),
            url=book_data.get("url", ""),
            thumb_url=book_data.get("thumb_url", ""),
            thumbnail=book_data.get("thumbnail", ""),
            assistant_id=book_data.get("assistant_id", ""),
            file_id=book_data.get("file_id", ""),
            vector_store_id=book_data.get("vector_store_id", ""),
            language=book_data.get("language", "en"),
            genres=book_data.get("genres", []),
            author=book_data.get("author", ""),
            pages=book_data.get("pages", 0),
            status=book_data.get("status", "active"),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        db.add(new_book)
        db.commit()
        db.refresh(new_book)
        return new_book
    except Exception as e:
        db.rollback()
        return None


def update_book(book_id: str, book_data: dict, db: Session):
    """Update an existing book"""
    try:
        book = db.query(Books).filter(Books.id == book_id).first()
        if not book:
            return None
        
        # Update only the fields that are provided
        for key, value in book_data.items():
            if hasattr(book, key) and value is not None:
                setattr(book, key, value)
        
        book.updated_at = datetime.now()
        db.commit()
        db.refresh(book)
        return book
    except Exception as e:
        db.rollback()
        return None


def delete_book(book_id: str, db: Session):
    """Delete a single book"""
    try:
        book = db.query(Books).filter(Books.id == book_id).first()
        if not book:
            return None
        db.delete(book)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        return None


def delete_bulk_books(book_ids: List[str], db: Session):
    """Delete multiple books by their IDs"""
    try:
        books = db.query(Books).filter(Books.id.in_(book_ids)).all()
        if not books:
            return None
        
        for book in books:
            db.delete(book)
        
        db.commit()
        return len(books)
    except Exception as e:
        db.rollback()
        return None


def get_books_analytics(db: Session):
    """Get books analytics including total count and count by status"""
    try:
        # Get total books count
        total_books = db.query(Books).count()
        
        # Get books count by status
        status_counts = db.query(
            Books.status,
            func.count(Books.id).label('count')
        ).group_by(Books.status).all()
        
        # Convert to dictionary format
        status_analytics = {}
        for status, count in status_counts:
            status_analytics[status] = count
        
        return {
            "total_books": total_books,
            "books_by_status": status_analytics
        }
    except Exception as e:
        db.rollback()
        return None


def get_books_by_status(status: str, db: Session, page: int = 1, limit: int = 20):
    """Get books filtered by status"""
    try:
        query = db.query(Books).filter(Books.status == status)
        total_count = query.count()
        books = query.offset((page - 1) * limit).limit(limit).all()
        
        return {
            "books": books,
            "total_count": total_count,
            "page": page,
            "limit": limit,
            "status": status
        }
    except Exception as e:
        db.rollback()
        return None 