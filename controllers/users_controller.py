from services.users_service import (
    get_all_users_by_school_id as db_get_all_users_by_school_id,
    get_user_by_id as db_get_user_by_id,
    add_user as db_add_user,
    update_user as db_update_user,
    delete_user as db_delete_user,
    delete_bulk_users as db_delete_bulk_users,
    get_users_by_school_name as db_get_users_by_school_name,
    get_users_by_registration_status as db_get_users_by_registration_status,
    get_user_by_ic_number as db_get_user_by_ic_number,
    get_users_with_school_id as db_get_users_with_school_id,
    get_user_statistics as db_get_user_statistics
)
from sqlalchemy.orm import Session
import json
from database.models import School
from database.connection import get_db


def serialize_user(user, db: Session = None):
    """Serialize user object to dictionary"""
    if not user:
        return None
    
    # Get school name if school_id exists
    school_name = None
    if user.school_id and db:
        try:
            school = db.query(School).filter(School.id == user.school_id).first()
            school_name = school.name if school else None
        except:
            pass
    
    return {
        "id": str(user.id),
        "ic_number": user.ic_number,
        "avatar_url": user.avatar_url,
        "name": user.name,
        "email": user.email if user.email else None,
        "birth": user.birth if user.birth else None,
        "address": user.address if user.address else None,
        "parent": user.parent if user.parent else None,
        "school_id": str(user.school_id) if user.school_id else None,
        "registration_status": user.registration_status,
        "rewards": user.rewards,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    }


def get_all_users_by_school_id(school_id: str, db: Session, page: int = 1, perPage: int = 20, sort: str = None, name: str = None, ic_number: str = None, status: str = None):
    """Get all users by school ID. If school_id is 'all', fetch all users"""
    result = db_get_all_users_by_school_id(school_id, db, page, perPage, sort, name, ic_number, status)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": f"Failed to fetch users for school_id: {school_id}",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "users": [serialize_user(user) for user in result["users"]],
            "total_count": result["total_count"],
            "total_students": result["total_students"],
            "page": result["page"],
            "perPage": result["perPage"],
            "school_id": result["school_id"]
        },
        "message": f"Users fetched successfully for school_id: {school_id}",
        "error": None
    }


def get_user_by_id(user_id: str, db: Session):
    """Get a single user by ID"""
    user = db_get_user_by_id(user_id, db)
    
    if not user:
        return {
            "success": False,
            "data": None,
            "message": f"User with id {user_id} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": {"user": serialize_user(user)},
        "message": "User fetched successfully",
        "error": None
    }


def add_user(user_data: dict, db: Session):
    """Add a new user"""
    user = db_add_user(user_data, db)
    
    if not user:
        return {
            "success": False,
            "data": None,
            "message": "Failed to add user",
            "error": "ADD_FAILED"
        }
    
    return {
        "success": True,
        "data": {"user": serialize_user(user)},
        "message": "User added successfully",
        "error": None
    }


def update_user(user_id: str, user_data: dict):
    """Update an existing user"""
    db: Session = next(get_db())
    user = db_update_user(user_id, user_data, db)
    
    if not user:
        return {
            "success": False,
            "data": None,
            "message": f"User with id {user_id} not found or update failed",
            "error": "UPDATE_FAILED"
        }
    
    return {
        "success": True,
        "data": {"user": serialize_user(user)},
        "message": "User updated successfully",
        "error": None
    }


def delete_user(user_id: str):
    """Delete a single user"""
    db: Session = next(get_db())
    result = db_delete_user(user_id, db)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": f"User with id {user_id} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": {"deleted_count": 1},
        "message": "User deleted successfully",
        "error": None
    }


def delete_bulk_users(user_ids: list, db: Session):
    """Delete multiple users"""
    deleted_count = db_delete_bulk_users(user_ids, db)
    
    if deleted_count is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to delete users",
            "error": "DELETE_FAILED"
        }
    
    return {
        "success": True,
        "data": {"deleted_count": deleted_count},
        "message": f"Successfully deleted {deleted_count} users",
        "error": None
    }


def get_users_by_school_name(school_name: str, db: Session, page: int = 1, limit: int = 20):
    """Get users filtered by school name"""
    result = db_get_users_by_school_name(school_name, db, page, limit)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch users by school name",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "users": [serialize_user(user) for user in result["users"]],
            "total_count": result["total_count"],
            "page": result["page"],
            "limit": result["limit"],
            "school_name": result["school_name"]
        },
        "message": f"Users with school '{school_name}' fetched successfully",
        "error": None
    }


def get_users_by_registration_status(status: str, db: Session, page: int = 1, limit: int = 20):
    """Get users filtered by registration status"""
    result = db_get_users_by_registration_status(status, db, page, limit)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch users by registration status",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "users": [serialize_user(user) for user in result["users"]],
            "total_count": result["total_count"],
            "page": result["page"],
            "limit": result["limit"],
            "status": result["status"]
        },
        "message": f"Users with status '{status}' fetched successfully",
        "error": None
    }


def get_user_by_ic_number(ic_number: str, db: Session):
    """Get user by IC number"""
    user = db_get_user_by_ic_number(ic_number, db)
    
    if not user:
        return {
            "success": False,
            "data": None,
            "message": f"User with IC number {ic_number} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": {"user": serialize_user(user)},
        "message": "User fetched successfully",
        "error": None
    }


def get_users_with_school_id(db: Session, page: int = 1, limit: int = 20):
    """Get all users with school_id filled based on school name"""
    result = db_get_users_with_school_id(db, page, limit)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch users with school_id",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "users": result["users"],
            "total_count": result["total_count"],
            "page": result["page"],
            "limit": result["limit"]
        },
        "message": "Users with school_id fetched successfully",
        "error": None
    }


def get_user_statistics(user_id: str, db: Session):
    """Get comprehensive user reading statistics"""
    result = db_get_user_statistics(user_id, db)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": f"User with id {user_id} not found or failed to fetch statistics",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": result,
        "message": "User statistics fetched successfully",
        "error": None
    } 