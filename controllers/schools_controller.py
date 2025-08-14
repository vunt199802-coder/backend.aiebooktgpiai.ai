from services.schools_service import (
    get_all_schools as db_get_all_schools,
    get_school_by_id as db_get_school_by_id,
    add_school as db_add_school,
    update_school as db_update_school,
    delete_school as db_delete_school,
    delete_bulk_schools as db_delete_bulk_schools,
    get_schools_analytics as db_get_schools_analytics,
    get_schools_by_status as db_get_schools_by_status,
    get_school_analytics_by_id as db_get_school_analytics_by_id
)
from services.leaderboard import get_school_leaderboard
from sqlalchemy.orm import Session
from database.models import School
import json
from typing import Optional


def serialize_school(school):
    """Serialize school object to dictionary"""
    if not school:
        return None
    return {
        "id": str(school.id),
        "name": school.name,
        "state": school.state,
        "city": school.city,
        "status": school.status,
        "created_at": school.created_at.isoformat() if school.created_at else None,
        "updated_at": school.updated_at.isoformat() if school.updated_at else None
    }


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
        "school": school_name,
        "school_id": str(user.school_id) if user.school_id else None,
        "registration_status": user.registration_status,
        "rewards": user.rewards,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None
    }


def get_all_schools(db: Session, page: int = 1, limit: int = 20, sort: Optional[str] = None, name: Optional[str] = None, state: Optional[str] = None, city: Optional[str] = None):
    """Get all schools with their students count"""
    result = db_get_all_schools(db, page, limit, sort, name, state, city)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch schools",
            "error": "DATABASE_ERROR"
        }
    
    # Process schools with their student counts
    schools_data = []
    for school, students_count in result["schools"]:
        school_data = serialize_school(school)
        school_data["students_count"] = students_count
        schools_data.append(school_data)
    
    return {
        "success": True,
        "data": {
            "schools": schools_data,
            "total_count": result["total_count"],
            "page": result["page"],
            "limit": result["limit"]
        },
        "message": "Schools fetched successfully",
        "error": None
    }


def get_school_by_id(school_id: str, db: Session):
    """Get a single school by ID with students data"""
    result = db_get_school_by_id(school_id, db)
    
    if not result:
        return {
            "success": False,
            "data": None,
            "message": f"School with id {school_id} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": {
            "school": serialize_school(result["school"]),
            "students_count": result["students_count"]
        },
        "message": "School fetched successfully",
        "error": None
    }


def add_school(school_data: dict, db: Session):
    """Add a new school"""
    school = db_add_school(school_data, db)
    
    if not school:
        return {
            "success": False,
            "data": None,
            "message": "Failed to add school",
            "error": "ADD_FAILED"
        }
    
    return {
        "success": True,
        "data": {"school": serialize_school(school)},
        "message": "School added successfully",
        "error": None
    }


def update_school(school_id: str, school_data: dict, db: Session):
    """Update an existing school"""
    school = db_update_school(school_id, school_data, db)
    
    if not school:
        return {
            "success": False,
            "data": None,
            "message": f"School with id {school_id} not found or update failed",
            "error": "UPDATE_FAILED"
        }
    
    return {
        "success": True,
        "data": {"school": serialize_school(school)},
        "message": "School updated successfully",
        "error": None
    }


def delete_school(school_id: str, db: Session):
    """Delete a single school"""
    result = db_delete_school(school_id, db)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": f"School with id {school_id} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": {"deleted_count": 1},
        "message": "School deleted successfully",
        "error": None
    }


def delete_bulk_schools(school_ids: list, db: Session):
    """Delete multiple schools"""
    deleted_count = db_delete_bulk_schools(school_ids, db)
    
    if deleted_count is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to delete schools",
            "error": "DELETE_FAILED"
        }
    
    return {
        "success": True,
        "data": {"deleted_count": deleted_count},
        "message": f"Successfully deleted {deleted_count} schools",
        "error": None
    }


def get_schools_analytics(db: Session, page: int = 1, perPage: int = 20, name: Optional[str] = None, state: Optional[str] = None, city: Optional[str] = None, sort: Optional[str] = None):
    """Get schools analytics with enhanced data"""
    analytics = db_get_schools_analytics(db, page, perPage, name, state, city, sort)
    
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


def get_schools_by_status(status: str, db: Session, page: int = 1, limit: int = 20):
    """Get schools filtered by status"""
    result = db_get_schools_by_status(status, db, page, limit)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch schools by status",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "schools": [serialize_school(school) for school in result["schools"]],
            "total_count": result["total_count"],
            "page": result["page"],
            "limit": result["limit"],
            "status": result["status"]
        },
        "message": f"Schools with status '{status}' fetched successfully",
        "error": None
    }


def get_school_analytics_by_id(school_id: str, db: Session):
    """Get analytics for a single school by ID with enhanced data"""
    result = db_get_school_analytics_by_id(school_id, db)
    
    if not result:
        return {
            "success": False,
            "data": None,
            "message": f"School with id {school_id} not found",
            "error": "NOT_FOUND"
        }
    
    return {
        "success": True,
        "data": result,
        "message": "School analytics fetched successfully",
        "error": None
    }


def get_school_leaderboard_controller(school_id: str, db: Session, page: int = 1, limit: int = 20):
    """Get leaderboard for a specific school"""
    # Validate UUID format first
    import uuid
    try:
        uuid.UUID(school_id)
    except ValueError:
        return {
            "success": False,
            "data": None,
            "message": f"Invalid school_id format: {school_id}. Must be a valid UUID.",
            "error": "INVALID_UUID_FORMAT"
        }
    
    # Validate school exists
    try:
        school = db.query(School).filter(School.id == school_id).first()
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "message": f"Database error while validating school: {str(e)}",
            "error": "DATABASE_ERROR"
        }
    
    if not school:
        return {
            "success": False,
            "data": None,
            "message": f"School with id {school_id} not found",
            "error": "SCHOOL_NOT_FOUND"
        }

    # Get leaderboard data from service
    try:
        result = get_school_leaderboard(school_id, db, page, limit)
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "message": f"Error while fetching leaderboard: {str(e)}",
            "error": "SERVICE_ERROR"
        }
    
    if not result["success"]:
        return result
    
    # Serialize leaderboard entries
    leaderboard_data = result["data"]
    if leaderboard_data and "leaderboard" in leaderboard_data:
        leaderboard_data["leaderboard"] = [
            serialize_leaderboard_entry(entry) for entry in leaderboard_data["leaderboard"]
        ]
    
    return {
        "success": True,
        "data": {
            "leaderboard": leaderboard_data["leaderboard"],
            "total_count": leaderboard_data["total_count"],
            "page": leaderboard_data["page"],
            "limit": leaderboard_data["limit"],
            "school_id": leaderboard_data["school_id"],
            "school_name": school.name
        },
        "message": f"Leaderboard fetched successfully for {school.name}",
        "error": None
    }


def serialize_leaderboard_entry(entry):
    """Serialize leaderboard entry to dictionary"""
    if not entry:
        return None
    
    return {
        "rank": entry.get("rank"),
        "user_id": entry.get("user_id"),
        "name": entry.get("name"),
        "ic_number": entry.get("ic_number"),
        "avatar_url": entry.get("avatar_url"),
        "total_score": entry.get("total_score"),
        "reading_sessions": entry.get("reading_sessions")
    }