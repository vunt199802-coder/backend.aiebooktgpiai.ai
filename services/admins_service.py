from sqlalchemy.orm import Session, joinedload
from database.models import Admin, School
from sqlalchemy import distinct
import uuid
from datetime import datetime
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def is_valid_uuid(uuid_string):
    """Validate if a string is a valid UUID format"""
    try:
        # Accept already-parsed UUID objects
        if isinstance(uuid_string, uuid.UUID):
            return True
        uuid.UUID(uuid_string)
        return True
    except ValueError:
        return False


def parse_uuid_or_none(value):
    """Return a uuid.UUID if value is a valid UUID string/object; otherwise None for empty/invalid inputs."""
    if value in (None, ""):  # normalize empty to None
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError):
        return None


def get_all_admins(db: Session):
    try:
        return db.query(Admin).options(joinedload(Admin.school)).all()
    except Exception as e:
        db.rollback()
        return None


def get_admin_by_id(admin_id: str, db: Session):
    try:
        return db.query(Admin).options(joinedload(Admin.school)).filter(Admin.id == admin_id).first()
    except Exception as e:
        db.rollback()
        return None


def get_admin_by_email(email: str, db: Session):
    try:
        return db.query(Admin).options(joinedload(Admin.school)).filter(Admin.email == email).first()
    except Exception as e:
        db.rollback()
        return None


def add_admin(admin_data: dict, db: Session):
    try:
        new_admin = Admin(
            id=admin_data.get("id", uuid.uuid4()),
            email=admin_data["email"],
            status=admin_data.get("status", "pending"),
            role=admin_data.get("role", "school_manager"),
            current_role=admin_data.get("current_role", admin_data.get("role", "school_manager")),
            createdAt=datetime.now(),
            updatedAt=datetime.now(),
            name=admin_data.get("name", ""),
            school_id=parse_uuid_or_none(admin_data.get("school_id")),
        )
        db.add(new_admin)
        db.commit()
        db.refresh(new_admin)
        return new_admin
    except Exception as e:
        db.rollback()
        return None


def update_admin(admin_id: str, admin_data: dict, db: Session):
    try:
        logger.info(f"Attempting to update admin with ID: {admin_id}")
        logger.info(f"Admin data to update: {admin_data}")
        
        # Validate UUID format before querying database
        if not is_valid_uuid(admin_id):
            logger.error(f"Invalid UUID format provided: {admin_id}")
            logger.error(f"UUID should be in format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
            return None
        
        admin = db.query(Admin).filter(Admin.id == admin_id).first()
        if not admin:
            logger.warning(f"Admin with ID {admin_id} not found")
            return None
            
        logger.info(f"Found admin: {admin.email} (ID: {admin.id})")
        
        # Log which fields are being updated
        updated_fields = []
        for key, value in admin_data.items():
            # Skip unknown fields
            if not hasattr(admin, key):
                logger.warning(f"Field '{key}' does not exist on Admin model, skipping")
                continue

            # Prevent updating immutable/system fields directly
            if key in {"id", "createdAt", "updatedAt"}:
                logger.info(f"Skipping immutable field '{key}'")
                continue

            # Special handling for relationship/foreign key fields
            if key == "school":
                # Do not assign raw strings to relationship; skip unless provided as None to clear
                if value in (None, ""):
                    old_value = admin.school
                    admin.school = None
                    updated_fields.append(f"school: {old_value} -> None")
                    logger.info(f"Updating field school: {old_value} -> None")
                else:
                    logger.info("Skipping direct assignment to 'school' relationship; provide 'school_id' to update")
                continue

            if key == "school_id":
                old_value = admin.school_id
                parsed_uuid = parse_uuid_or_none(value)
                admin.school_id = parsed_uuid
                if parsed_uuid is None:
                    # Also clear relationship if FK cleared
                    admin.school = None
                updated_fields.append(f"school_id: {old_value} -> {parsed_uuid}")
                logger.info(f"Updating field school_id: {old_value} -> {parsed_uuid}")
                continue

            # Default: direct assignment for scalar fields
            old_value = getattr(admin, key)
            setattr(admin, key, value)
            updated_fields.append(f"{key}: {old_value} -> {value}")
            logger.info(f"Updating field {key}: {old_value} -> {value}")
        
        admin.updatedAt = datetime.now()
        logger.info(f"Updated {len(updated_fields)} fields: {', '.join(updated_fields)}")
        
        db.commit()
        db.refresh(admin)
        logger.info(f"Successfully updated admin {admin_id}")
        return admin
    except Exception as e:
        logger.error(f"Error updating admin {admin_id}: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Admin data that caused error: {admin_data}")
        logger.error(f"Exception details: {repr(e)}")
        
        # Handle specific database errors
        if "DataError" in str(type(e).__name__) or "InvalidTextRepresentation" in str(e):
            logger.error(f"Database data error - likely invalid UUID format or data type mismatch")
            logger.error(f"Admin ID provided: {admin_id}")
            logger.error(f"Check if admin_id is a valid UUID format")
        
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        db.rollback()
        return None


def delete_admin(admin_id: str, db: Session):
    try:
        admin = db.query(Admin).filter(Admin.id == admin_id).first()
        if not admin:
            return None
        db.delete(admin)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        return None


def get_all_schools(db: Session):
    try:
        # Get distinct schools from users table, excluding None and empty values
        schools = db.query(School.name).all()
        # Extract school names from the result tuples
        return [school[0] for school in schools if school[0]]
    except Exception as e:
        db.rollback()
        return []


def update_admin_last_login(email: str, db: Session):
    try:
        admin = db.query(Admin).options(joinedload(Admin.school)).filter(Admin.email == email).first()
        if not admin:
            return None
        admin.last_login = datetime.now()
        admin.updatedAt = datetime.now()
        db.commit()
        db.refresh(admin)
        return admin
    except Exception as e:
        db.rollback()
        return None 