from fastapi import APIRouter, Path, Body, Query, File, UploadFile, Depends
from sqlalchemy.orm import Session
from dependencies import get_db_session
from controllers.books_controller import (
    get_all_books,
    get_book_by_id,
    add_book,
    update_book,
    delete_book,
    delete_bulk_books,
    get_books_analytics,
    upload_book,
    get_books_by_status
)

router = APIRouter(prefix="/api/books", tags=["books"])

@router.get("/", summary="Get all books")
def route_get_all_books(
    page: int = Query(1, ge=1, description="Page number"),
    perPage: int = Query(20, ge=1, le=100, description="Number of items per page"),
    status: str = Query(None, description="Filter by book status"),
    db: Session = Depends(get_db_session)
):
    """Get all books with pagination and optional status filtering"""
    return get_all_books(db=db, page=page, limit=perPage, status=status)

@router.get("/by_id/{book_id}", summary="Get one book by ID")
def route_get_book_by_id(
    book_id: str = Path(..., description="Book ID"),
    db: Session = Depends(get_db_session)
):
    """Get a single book by its ID"""
    return get_book_by_id(book_id, db)

@router.get("/by_status/{status}", summary="Get books by status")
def route_get_books_by_status(
    status: str = Path(..., description="Book status to filter by"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Number of items per page"),
    db: Session = Depends(get_db_session)
):
    """Get books filtered by status"""
    return get_books_by_status(status, db=db, page=page, limit=limit)

@router.post("/upload", summary="Upload a book file")
def route_upload_book(
    file: UploadFile = File(..., description="Book file to upload"),
    db: Session = Depends(get_db_session)
):
    """Upload a book file (PDF, EPUB, etc.)"""
    return upload_book(file)

@router.post("/", summary="Add a new book")
def route_add_book(
    book_data: dict = Body(..., description="Book data"),
    db: Session = Depends(get_db_session)
):
    """Add a new book to the database"""
    return add_book(book_data, db)

@router.patch("/{book_id}", summary="Update book information")
def route_update_book(
    book_id: str = Path(..., description="Book ID"),
    book_data: dict = Body(..., description="Updated book data"),
    db: Session = Depends(get_db_session)
):
    """Update an existing book's information"""
    return update_book(book_id, book_data, db)

@router.delete("/{book_id}", summary="Delete one book")
def route_delete_book(
    book_id: str = Path(..., description="Book ID"),
    db: Session = Depends(get_db_session)
):
    """Delete a single book by ID"""
    return delete_book(book_id, db)

@router.delete("/bulk", summary="Delete multiple books")
def route_delete_bulk_books(
    book_ids: list = Body(..., description="List of book IDs to delete"),
    db: Session = Depends(get_db_session)
):
    """Delete multiple books by their IDs"""
    return delete_bulk_books(book_ids, db)

@router.get("/analytics", summary="Get books analytics")
def route_get_books_analytics(
    db: Session = Depends(get_db_session)
):
    """Get books analytics including total count and count by status"""
    return get_books_analytics(db) 