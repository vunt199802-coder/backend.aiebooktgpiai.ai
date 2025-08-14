from services.admins_service import (
    get_all_admins as db_get_all_admins,
    get_admin_by_id as db_get_admin_by_id,
    add_admin as db_add_admin,
    update_admin as db_update_admin,
    delete_admin as db_delete_admin,
    get_admin_by_email as db_get_admin_by_email,
    get_all_schools as db_get_all_schools,
    update_admin_last_login as db_update_admin_last_login,
)
from database.connection import get_db
from sqlalchemy.orm import Session

def serialize_admin(admin):
    if not admin:
        return None
    return {
        "id": str(admin.id),
        "email": admin.email,
        "status": admin.status,
        "role": admin.role,
        "current_role": admin.current_role,
        "createdAt": admin.createdAt.isoformat() if admin.createdAt else None,
        "updatedAt": admin.updatedAt.isoformat() if admin.updatedAt else None,
        "name": admin.name,
        "last_login": admin.last_login.isoformat() if admin.last_login else None,
        "school_id": admin.school_id,
        "school": admin.school,
    }

def get_all_admins(page: int = 1, page_limit: int = 20):
    db: Session = next(get_db())
    admins = db_get_all_admins(db)
    total_count = len(admins)
    return {
        "success": True,
        "data": {
            "admins": [serialize_admin(a) for a in admins],
            "total_count": total_count,
            "page": page,
            "page_limit": page_limit
        },
        "message": "Admins fetched successfully",
        "error": None
    }

def get_admin_by_id(admin_id: str):
    db: Session = next(get_db())
    admin = db_get_admin_by_id(admin_id, db)
    if not admin:
        return {
            "success": False,
            "data": None,
            "message": f"Admin with id {admin_id} not found",
            "error": "NOT_FOUND"
        }
    return {
        "success": True,
        "data": {"admin": serialize_admin(admin)},
        "message": "Admin fetched successfully",
        "error": None
    }

def get_admin_by_email(email: str):
    db: Session = next(get_db())
    admin = db_get_admin_by_email(email, db)
    if not admin:
        return {
            "success": False,
            "data": None,
            "message": f"Admin with email {email} not found",
            "error": "NOT_FOUND"
        }
    return {
        "success": True,
        "data": {"admin": serialize_admin(admin)},
        "message": "Admin fetched successfully",
        "error": None
    }

def add_admin(admin_data: dict):
    db: Session = next(get_db())
    admin = db_add_admin(admin_data, db)
    if not admin:
        return {
            "success": False,
            "data": None,
            "message": "Failed to add admin",
            "error": "ADD_FAILED"
        }
    return {
        "success": True,
        "data": {"admin": serialize_admin(admin)},
        "message": "Admin added successfully",
        "error": None
    }

def update_admin(admin_id: str, admin_data: dict):
    db: Session = next(get_db())
    admin = db_update_admin(admin_id, admin_data, db)
    if not admin:
        return {
            "success": False,
            "data": None,
            "message": f"Admin with id {admin_id} not found or update failed",
            "error": "UPDATE_FAILED"
        }
    return {
        "success": True,
        "data": {"admin": serialize_admin(admin)},
        "message": "Admin updated successfully",
        "error": None
    }

def delete_admin(admin_id: str):
    db: Session = next(get_db())
    result = db_delete_admin(admin_id, db)
    if not result:
        return {
            "success": False,
            "data": None,
            "message": f"Admin with id {admin_id} not found or delete failed",
            "error": "DELETE_FAILED"
        }
    return {
        "success": True,
        "data": None,
        "message": "Admin deleted successfully",
        "error": None
    }

def get_all_schools():
    db: Session = next(get_db())
    schools = db_get_all_schools(db)
    return {
        "success": True,
        "data": {
            "schools": schools,
            "total_count": len(schools)
        },
        "message": "Schools fetched successfully",
        "error": None
    } 

    
def count_signin(admin_data: dict):
    db: Session = next(get_db())
    email = admin_data["email"],
    admin = db_update_admin_last_login(email, db)
    if not admin:
        return {
            "success": False,
            "data": None,
            "message": f"Admin with email {email} not found",
            "error": "NOT_FOUND"
        }
    return {
        "success": True,
        "data": {"admin": serialize_admin(admin)},
        "message": "Admin last login updated successfully",
        "error": None
    }