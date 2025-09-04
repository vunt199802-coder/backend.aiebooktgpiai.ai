from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from database.models import User, School, ReadingHistory, Books, FavoriteBooks
import uuid
from datetime import datetime
from typing import List, Optional
import logging
import json

# Configure logging
logger = logging.getLogger(__name__)

def _is_json_field(field_name: str, value) -> bool:
    """Check if a field should be treated as JSON based on its value and name"""
    # Common JSON field names
    json_field_names = {'parent', 'metadata', 'settings', 'config', 'data', 'attributes'}
    
    # Check if field name suggests it's JSON
    if field_name.lower() in json_field_names:
        logger.debug(f"Field '{field_name}' identified as JSON by field name")
        return True
    
    # Check if value is a dict (common JSON structure)
    if isinstance(value, dict):
        logger.debug(f"Field '{field_name}' identified as JSON by dict value type")
        return True
    
    # Check if value is a list (also common JSON structure)
    if isinstance(value, list):
        logger.debug(f"Field '{field_name}' identified as JSON by list value type")
        return True
    
    # Check if value is already a JSON string
    if isinstance(value, str) and (value.startswith('{') or value.startswith('[')):
        try:
            json.loads(value)
            logger.debug(f"Field '{field_name}' is already a valid JSON string, skipping serialization")
            return False  # Already a JSON string, don't serialize again
        except (json.JSONDecodeError, ValueError):
            logger.debug(f"Field '{field_name}' starts with {{ or [ but is not valid JSON, treating as regular string")
            pass
    
    return False


def deserialize_json_field(value):
    """Helper function to deserialize JSON fields when retrieving from database
    
    Use this function when you need to convert JSON strings back to Python objects
    after retrieving them from the database.
    
    Args:
        value: The value from the database (could be JSON string or other type)
        
    Returns:
        Deserialized Python object if it was JSON, otherwise the original value
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


def get_all_users_by_school_id(school_id: str, db: Session, page: int = 1, perPage: int = 20, sort: str = None, name: str = None, ic_number: str = None, status: str = None):
    """Get all users by school ID. If school_id is 'all', fetch all users"""
    try:
        query = db.query(User)
        
        if school_id != 'all':
            query = query.filter(User.school_id == school_id)
        
        # Apply name filter
        if name:
            query = query.filter(User.name.ilike(f"%{name}%"))
        
        # Apply IC number filter
        if ic_number:
            query = query.filter(User.ic_number.ilike(f"%{ic_number}%"))
        
        # Apply status filter
        if status:
            query = query.filter(User.registration_status == status)
        
        # Apply sorting
        if sort:
            import json
            try:
                sort_data = json.loads(sort)
                if isinstance(sort_data, list) and len(sort_data) > 0:
                    sort_item = sort_data[0]
                    sort_field = sort_item.get("id")
                    sort_desc = sort_item.get("desc", False)
                    
                    if sort_field == "name":
                        if sort_desc:
                            query = query.order_by(User.name.desc())
                        else:
                            query = query.order_by(User.name.asc())
                    elif sort_field == "ic_number":
                        if sort_desc:
                            query = query.order_by(User.ic_number.desc())
                        else:
                            query = query.order_by(User.ic_number.asc())
                    elif sort_field == "created_at":
                        if sort_desc:
                            query = query.order_by(User.created_at.desc())
                        else:
                            query = query.order_by(User.created_at.asc())
                    elif sort_field == "registration_status":
                        if sort_desc:
                            query = query.order_by(User.registration_status.desc())
                        else:
                            query = query.order_by(User.registration_status.asc())
            except (json.JSONDecodeError, KeyError, TypeError):
                # If sorting fails, use default ordering
                query = query.order_by(User.created_at.desc())
        else:
            # Default sorting by created_at desc
            query = query.order_by(User.created_at.desc())
        
        total_count = query.count()
        users = query.offset((page - 1) * perPage).limit(perPage).all()
        total_students = db.query(User).count()

        return {
            "users": users,
            "total_students": total_students,
            "total_count": total_count,
            "page": page,
            "perPage": perPage,
            "school_id": school_id
        }
    except Exception as e:
        db.rollback()
        return None


def get_user_by_id(user_id: str, db: Session):
    """Get a single user by ID"""
    try:
        return db.query(User).filter(User.id == user_id).first()
    except Exception as e:
        db.rollback()
        return None


def add_user(user_data: dict, db: Session):
    """Add a new user
    
    This function automatically handles JSON fields by converting Python dicts/lists
    to JSON strings before storing them in the database.
    """
    logger.info(f"Starting user creation with data: {user_data}")
    
    try:
        # Process user data to handle JSON fields
        processed_data = {}
        for key, value in user_data.items():
            if _is_json_field(key, value):
                logger.debug(f"Processing JSON field '{key}' for new user")
                try:
                    processed_data[key] = json.dumps(value)
                    logger.info(f"Serialized JSON field '{key}' for new user")
                except Exception as json_error:
                    logger.error(f"Failed to serialize JSON for field '{key}': {json_error}")
                    # Continue with other fields, but log the error
                    processed_data[key] = value
            else:
                processed_data[key] = value
        
        new_user = User(
            id=processed_data.get("id", uuid.uuid4()),
            ic_number=processed_data["ic_number"],
            avatar_url=processed_data.get("avatar_url", ""),
            name=processed_data["name"],
            birth=processed_data.get("birth"),
            address=processed_data.get("address"),
            parent=processed_data.get("parent"),
            school_id=processed_data.get("school_id"),
            registration_status=processed_data.get("registration_status", "approved"),
            rewards=processed_data.get("rewards", []),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        logger.info(f"Creating new user: {new_user.name} (ID: {new_user.id})")
        db.add(new_user)
        db.commit()
        logger.info("User created successfully in database")
        
        db.refresh(new_user)
        logger.info("User object refreshed from database")
        
        return new_user
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}", exc_info=True)
        db.rollback()
        return None


def update_user(user_id: str, user_data: dict, db: Session):
    """Update an existing user
    
    This function automatically handles JSON fields by converting Python dicts/lists
    to JSON strings before storing them in the database. Fields like 'parent' 
    that contain structured data will be automatically serialized.
    """
    logger.info(f"Starting user update for user_id: {user_id}")
    logger.info(f"Update data received: {user_data}")
    
    try:
        # Log the query attempt
        logger.debug(f"Querying database for user with ID: {user_id}")
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            logger.warning(f"User not found with ID: {user_id}")
            return None
        
        logger.info(f"Found user: {user.name} (ID: {user.id})")
        logger.info(f"Current user data before update: {user.__dict__}")
        
        # Track which fields are being updated
        updated_fields = []
        skipped_fields = []
        
        # Update only the fields that are provided
        for key, value in user_data.items():
            if hasattr(user, key) and value is not None:
                old_value = getattr(user, key)
                
                # Handle JSON fields (like parent field)
                if _is_json_field(key, value):
                    logger.debug(f"Detected JSON field '{key}' with value type: {type(value)}")
                    try:
                        # Convert dict/list to JSON string for storage
                        json_value = json.dumps(value)
                        setattr(user, key, json_value)
                        logger.info(f"Updated JSON field '{key}': {old_value} -> {json_value}")
                    except Exception as json_error:
                        logger.error(f"Failed to serialize JSON for field '{key}': {json_error}")
                        logger.error(f"JSON serialization failed for field '{key}' with value: {value}")
                        continue
                else:
                    setattr(user, key, value)
                    logger.info(f"Updated field '{key}': {old_value} -> {value}")
                
                updated_fields.append({
                    'field': key,
                    'old_value': old_value,
                    'new_value': value
                })
            else:
                if not hasattr(user, key):
                    logger.warning(f"Skipped field '{key}' - attribute does not exist on User model")
                elif value is None:
                    logger.debug(f"Skipped field '{key}' - value is None")
                skipped_fields.append(key)
        
        if updated_fields:
            logger.info(f"Total fields updated: {len(updated_fields)}")
            logger.info(f"Updated fields: {[field['field'] for field in updated_fields]}")
        else:
            logger.warning("No fields were updated")
        
        if skipped_fields:
            logger.info(f"Skipped fields: {skipped_fields}")
        
        # Update timestamp
        old_updated_at = user.updated_at
        user.updated_at = datetime.now()
        logger.info(f"Updated timestamp: {old_updated_at} -> {user.updated_at}")
        
        # Commit changes
        logger.debug("Committing changes to database")
        db.commit()
        logger.info("Database commit successful")
        
        # Refresh user object
        logger.debug("Refreshing user object from database")
        db.refresh(user)
        logger.info("User object refreshed successfully")
        
        logger.info(f"User update completed successfully for user_id: {user_id}")
        return user
        
    except Exception as e:
        logger.error(f"Error updating user {user_id}: {str(e)}", exc_info=True)
        logger.error(f"Rolling back database transaction")
        db.rollback()
        return None


def delete_user(user_id: str, db: Session):
    """Delete a single user"""
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return None
        db.delete(user)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        return None


def delete_bulk_users(user_ids: List[str], db: Session):
    """Delete multiple users by their IDs"""
    try:
        users = db.query(User).filter(User.id.in_(user_ids)).all()
        if not users:
            return None
        
        for user in users:
            db.delete(user)
        
        db.commit()
        return len(users)
    except Exception as e:
        db.rollback()
        return None


def get_users_by_school_name(school_name: str, db: Session, page: int = 1, limit: int = 20):
    """Get users filtered by school name"""
    try:
        # First get the school by name
        school = db.query(School).filter(School.name == school_name).first()
        if not school:
            return None
        
        # Then get users by school_id
        query = db.query(User).filter(User.school_id == school.id)
        total_count = query.count()
        users = query.offset((page - 1) * limit).limit(limit).all()
        
        return {
            "users": users,
            "total_count": total_count,
            "page": page,
            "limit": limit,
            "school_name": school_name
        }
    except Exception as e:
        db.rollback()
        return None


def get_users_by_registration_status(status: str, db: Session, page: int = 1, limit: int = 20):
    """Get users filtered by registration status"""
    try:
        query = db.query(User).filter(User.registration_status == status)
        total_count = query.count()
        users = query.offset((page - 1) * limit).limit(limit).all()
        
        return {
            "users": users,
            "total_count": total_count,
            "page": page,
            "limit": limit,
            "status": status
        }
    except Exception as e:
        db.rollback()
        return None


def get_user_by_ic_number(ic_number: str, db: Session):
    """Get user by IC number"""
    try:
        return db.query(User).filter(User.ic_number == ic_number).first()
    except Exception as e:
        db.rollback()
        return None


def get_users_with_school_id(db: Session, page: int = 1, limit: int = 20):
    """Get all users with their school information"""
    try:
        # Get all users with school information
        users = db.query(User).offset((page - 1) * limit).limit(limit).all()
        total_count = db.query(User).count()
        
        # Process users and add school information
        users_with_school_info = []
        for user in users:
            # Get school name if school_id exists
            school_name = None
            if user.school_id:
                school = db.query(School).filter(School.id == user.school_id).first()
                school_name = school.name if school else None
            
            user_data = {
                "id": str(user.id),
                "ic_number": user.ic_number,
                "avatar_url": user.avatar_url,
                "name": user.name,
                "school": school_name,
                "school_id": str(user.school_id) if user.school_id else None,
                "registration_status": user.registration_status,
                "rewards": user.rewards,
                "created_at": user.created_at,
                "updated_at": user.updated_at
            }
            users_with_school_info.append(user_data)
        
        return {
            "users": users_with_school_info,
            "total_count": total_count,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        db.rollback()
        return None


def get_user_statistics(user_id: str, db: Session):
    """Get comprehensive user reading statistics"""
    try:
        # Get main user information
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return None
        
        # Get school name if school_id exists
        school_name = None
        if user.school_id:
            school = db.query(School).filter(School.id == user.school_id).first()
            school_name = school.name if school else None
        
        # Get reading history with complete book information
        reading_data = db.query(
            ReadingHistory,
            Books
        ).join(
            Books, ReadingHistory.book_id == Books.id
        ).filter(
            ReadingHistory.user_id == user.id
        ).all()
        
        # Calculate statistics
        total_read_books = len(set(record[0].book_id for record in reading_data))
        total_reading_duration = sum(record[0].duration or 0 for record in reading_data)
        
        # Count books by language
        language_counts = {}
        read_books_list = []
        latest_timestamp = None
        
        # Group by book to avoid duplicates and get unique books
        unique_books = {}
        for reading_record, book in reading_data:
            book_id = reading_record.book_id
            if book_id not in unique_books:
                unique_books[book_id] = {
                    'book': book,
                    'latest_read': reading_record.started_at
                }
                # Create book object with all information
                book_obj = {
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
                read_books_list.append(book_obj)
            else:
                # Update latest read time if this record is more recent
                if (reading_record.started_at and 
                    (not unique_books[book_id]['latest_read'] or 
                     reading_record.started_at > unique_books[book_id]['latest_read'])):
                    unique_books[book_id]['latest_read'] = reading_record.started_at
        
        # Count languages from unique books
        for book_data in unique_books.values():
            language = book_data['book'].language or 'unknown'
            language_counts[language] = language_counts.get(language, 0) + 1
            
            # Track latest timestamp
            if book_data['latest_read']:
                if not latest_timestamp or book_data['latest_read'] > latest_timestamp:
                    latest_timestamp = book_data['latest_read']
        
        # Get language-specific counts
        malay_count = language_counts.get('malay', 0) + language_counts.get('Malay', 0)
        english_count = language_counts.get('english', 0) + language_counts.get('English', 0)
        mandarin_count = language_counts.get('mandarin', 0) + language_counts.get('Mandarin', 0) + language_counts.get('chinese', 0) + language_counts.get('Chinese', 0)
        
        return {
            "user_info": {
                "id": str(user.id),
                "ic_number": user.ic_number,
                "name": user.name,
                "email": user.email,
                "avatar_url": user.avatar_url,
                "school_id": str(user.school_id) if user.school_id else None,
                "school_name": school_name,
                "registration_status": user.registration_status,
                "created_at": user.created_at.isoformat() if user.created_at else None,
            },
            "reading_statistics": {
                "total_read_books_count": total_read_books,
                "malay_read_books_count": malay_count,
                "english_read_books_count": english_count,
                "mandarin_read_books_count": mandarin_count,
                "total_reading_duration": total_reading_duration,
                "read_books_list": read_books_list,
                "last_book_read_timestamp": latest_timestamp.isoformat() if latest_timestamp else None,
                "language_breakdown": language_counts
            }
        }
    except Exception as e:
        db.rollback()
        return None


def add_favorite_book(user_id: str, book_id: str, db: Session):
    """Add a book to user's favorites"""
    try:
        # Check if the favorite already exists
        existing_favorite = db.query(FavoriteBooks).filter(
            FavoriteBooks.user_id == user_id,
            FavoriteBooks.book_id == book_id
        ).first()
        
        if existing_favorite:
            return {"success": False, "data": "Book is already in favorites"}
        
        # Create new favorite entry
        new_favorite = FavoriteBooks(
            user_id=user_id,
            book_id=book_id
        )
        
        # Add to database
        db.add(new_favorite)
        db.commit()
        
        return {"success": True, "data": "Book added to favorites"}
        
    except Exception as e:
        db.rollback()
        return {"success": False, "data": str(e)}


def remove_favorite_book(user_id: str, book_id: str, db: Session):
    """Remove a book from user's favorites"""
    try:
        # Find user by IC number
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return {"success": False, "data": "User not found"}
        
        # Find and delete the favorite entry
        favorite = db.query(FavoriteBooks).filter(
            FavoriteBooks.user_id == user_id,
            FavoriteBooks.book_id == book_id
        ).first()
        
        if not favorite:
            return {"success": False, "data": "Book not found in favorites"}
            
        # Delete the favorite entry
        db.delete(favorite)
        db.commit()
        
        return {"success": True, "data": "Book removed from favorites"}
    
    except Exception as e:
        db.rollback()
        return {"success": False, "data": str(e)}


def get_favorite_books(user_id: str, db: Session):
    """Get user's favorite book IDs"""
    try:
        
        # Get favorite book IDs
        favorites = db.query(Books.id).filter(
            FavoriteBooks.user_id ==user_id
        ).join(
            FavoriteBooks,
            Books.id == FavoriteBooks.book_id
        ).all()
        
        # Convert list of tuples to list of IDs
        favorite_ids = [book_id[0] for book_id in favorites]
        
        return {"success": True, "data": favorite_ids}
        
    except Exception as e:
        db.rollback()
        return {"success": False, "data": "Error retrieving favorites"} 