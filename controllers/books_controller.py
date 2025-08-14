from services.books_service import (
    get_all_books as db_get_all_books,
    get_book_by_id as db_get_book_by_id,
    add_book as db_add_book,
    update_book as db_update_book,
    delete_book as db_delete_book,
    delete_bulk_books as db_delete_bulk_books,
    get_books_analytics as db_get_books_analytics,
    get_books_by_status as db_get_books_by_status
)
from sqlalchemy.orm import Session
from fastapi import UploadFile, File
import json


def serialize_book(book):
    """Serialize book object to dictionary"""
    if not book:
        return None
    return {
        "id": str(book.id),
        "title": book.title,
        "file_key": book.file_key,
        "url": book.url,
        "thumb_url": book.thumb_url,
        "thumbnail": book.thumbnail,
        "assistant_id": book.assistant_id,
        "file_id": book.file_id,
        "vector_store_id": book.vector_store_id,
        "language": book.language,
        "genres": book.genres,
        "author": book.author,
        "pages": book.pages,
        "status": book.status,
        "created_at": book.created_at.isoformat() if book.created_at else None,
        "updated_at": book.updated_at.isoformat() if book.updated_at else None
    }


def get_all_books(db: Session, page: int = 1, limit: int = 20, status: str = None):
    """Get all books with pagination and optional status filtering"""
    result = db_get_all_books(db, page, limit, status)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch books",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "books": [serialize_book(book) for book in result["books"]],
            "total_count": result["total_count"],
            "page": result["page"],
            "limit": result["limit"]
        },
        "message": "Books fetched successfully",
        "error": None
    }


def get_book_by_id(book_id: str, db: Session):
    """Get a single book by ID"""
    book = db_get_book_by_id(book_id, db)
    
    if not book:
        return {
            "success": False,
            "data": None,
            "message": f"Book with id {book_id} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": {"book": serialize_book(book)},
        "message": "Book fetched successfully",
        "error": None
    }


def add_book(book_data: dict, db: Session):
    """Add a new book"""
    book = db_add_book(book_data, db)
    
    if not book:
        return {
            "success": False,
            "data": None,
            "message": "Failed to add book",
            "error": "ADD_FAILED"
        }
    
    return {
        "success": True,
        "data": {"book": serialize_book(book)},
        "message": "Book added successfully",
        "error": None
    }


def update_book(book_id: str, book_data: dict, db: Session):
    """Update an existing book"""
    book = db_update_book(book_id, book_data, db)
    
    if not book:
        return {
            "success": False,
            "data": None,
            "message": f"Book with id {book_id} not found or update failed",
            "error": "UPDATE_FAILED"
        }
    
    return {
        "success": True,
        "data": {"book": serialize_book(book)},
        "message": "Book updated successfully",
        "error": None
    }


def delete_book(book_id: str, db: Session):
    """Delete a single book"""
    result = db_delete_book(book_id, db)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": f"Book with id {book_id} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": {"deleted_count": 1},
        "message": "Book deleted successfully",
        "error": None
    }


def delete_bulk_books(book_ids: list, db: Session):
    """Delete multiple books"""
    deleted_count = db_delete_bulk_books(book_ids, db)
    
    if deleted_count is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to delete books",
            "error": "DELETE_FAILED"
        }
    
    return {
        "success": True,
        "data": {"deleted_count": deleted_count},
        "message": f"Successfully deleted {deleted_count} books",
        "error": None
    }


def get_books_analytics(db: Session):
    """Get books analytics"""
    analytics = db_get_books_analytics(db)
    
    if analytics is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch analytics",
            "error": "ANALYTICS_ERROR"
        }
    
    return {
        "success": True,
        "data": analytics,
        "message": "Analytics fetched successfully",
        "error": None
    }


def upload_book(file: UploadFile = File(...)):
    """Handle book file upload"""
    try:
        # This is a placeholder for file upload logic
        # In a real implementation, you would:
        # 1. Save the file to storage (S3, local, etc.)
        # 2. Process the file (extract metadata, generate thumbnails, etc.)
        # 3. Create book record in database
        
        return {
            "success": True,
            "data": {
                "filename": file.filename,
                "size": file.size,
                "content_type": file.content_type
            },
            "message": "Book uploaded successfully",
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "message": f"Failed to upload book: {str(e)}",
            "error": "UPLOAD_FAILED"
        }


def get_books_by_status(status: str, db: Session, page: int = 1, limit: int = 20):
    """Get books filtered by status"""
    result = db_get_books_by_status(status, db, page, limit)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch books by status",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "books": [serialize_book(book) for book in result["books"]],
            "total_count": result["total_count"],
            "page": result["page"],
            "limit": result["limit"],
            "status": result["status"]
        },
        "message": f"Books with status '{status}' fetched successfully",
        "error": None
    } 