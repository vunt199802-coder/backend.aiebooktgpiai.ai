from sqlalchemy.orm import Session
from sqlalchemy import func, distinct, desc, asc
from database.models import School, User, ReadingHistory
import uuid
from datetime import datetime, timedelta
from typing import List, Optional
import json


def get_all_schools(db: Session, page: int = 1, limit: int = 20, sort: Optional[str] = None, name: Optional[str] = None, state: Optional[str] = None, city: Optional[str] = None):
    """Get all schools with their students count"""
    try:
        # Start with base query
        query = db.query(
            School,
            func.count(User.id).label('students_count')
        ).outerjoin(
            User, School.id == User.school_id
        )
        
        # Apply filters
        if name:
            query = query.filter(School.name.ilike(f"%{name}%"))
        
        if state:
            query = query.filter(School.state.ilike(f"%{state}%"))
        
        if city:
            query = query.filter(School.city.ilike(f"%{city}%"))
        
        # Apply sorting
        if sort:
            try:
                sort_params = json.loads(sort)
                if isinstance(sort_params, list):
                    for sort_item in sort_params:
                        if isinstance(sort_item, dict):
                            field = sort_item.get('id')
                            desc_order = sort_item.get('desc', False)
                            
                            if field == 'name':
                                if desc_order:
                                    query = query.order_by(desc(School.name))
                                else:
                                    query = query.order_by(asc(School.name))
                            elif field == 'state':
                                if desc_order:
                                    query = query.order_by(desc(School.state))
                                else:
                                    query = query.order_by(asc(School.state))
                            elif field == 'city':
                                if desc_order:
                                    query = query.order_by(desc(School.city))
                                else:
                                    query = query.order_by(asc(School.city))
                            elif field == 'status':
                                if desc_order:
                                    query = query.order_by(desc(School.status))
                                else:
                                    query = query.order_by(asc(School.status))
                            elif field == 'created_at':
                                if desc_order:
                                    query = query.order_by(desc(School.created_at))
                                else:
                                    query = query.order_by(asc(School.created_at))
                            elif field == 'students_count':
                                if desc_order:
                                    query = query.order_by(desc(func.count(User.id)))
                                else:
                                    query = query.order_by(asc(func.count(User.id)))
            except (json.JSONDecodeError, KeyError, TypeError):
                # If sorting fails, use default sorting by name
                query = query.order_by(asc(School.name))
        else:
            # Default sorting by name
            query = query.order_by(asc(School.name))
        
        # Group by school and apply pagination
        schools_with_count = query.group_by(
            School.id
        ).offset(
            (page - 1) * limit
        ).limit(
            limit
        ).all()
        
        # When not filtering by ic_number, we can use the simpler approach
        count_query = db.query(School)
        if name:
            count_query = count_query.filter(School.name.ilike(f"%{name}%"))
        if state:
            count_query = count_query.filter(School.state.ilike(f"%{state}%"))
        if city:
            count_query = count_query.filter(School.city.ilike(f"%{city}%"))
        total_count = count_query.count()
        
        return {
            "schools": schools_with_count,
            "total_count": total_count,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        db.rollback()
        return None


def get_school_by_id(school_id: str, db: Session):
    """Get a single school by ID with students data"""
    try:
        print('school_id', school_id)
        school = db.query(School).filter(School.id == school_id).first()
        if not school:
            return None
        
        # Get students for this school
        students = db.query(User).filter(User.school_id == school_id).all()
        
        return {
            "school": school,
            "students_count": len(students)
        }
    except Exception as e:
        db.rollback()
        return None


def add_school(school_data: dict, db: Session):
    """Add a new school"""
    try:
        new_school = School(
            id=school_data.get("id", uuid.uuid4()),
            name=school_data["name"],
            state=school_data.get("state", ""),
            city=school_data.get("city", ""),
            status=school_data.get("status", "active"),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        db.add(new_school)
        db.commit()
        db.refresh(new_school)
        return new_school
    except Exception as e:
        db.rollback()
        return None


def update_school(school_id: str, school_data: dict, db: Session):
    """Update an existing school"""
    try:
        school = db.query(School).filter(School.id == school_id).first()
        if not school:
            return None
        
        # Update only the fields that are provided
        for key, value in school_data.items():
            if hasattr(school, key) and value is not None:
                setattr(school, key, value)
        
        school.updated_at = datetime.now()
        db.commit()
        db.refresh(school)
        return school
    except Exception as e:
        db.rollback()
        return None


def delete_school(school_id: str, db: Session):
    """Delete a single school"""
    try:
        school = db.query(School).filter(School.id == school_id).first()
        if not school:
            return None
        db.delete(school)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        return None


def delete_bulk_schools(school_ids: List[str], db: Session):
    """Delete multiple schools by their IDs"""
    try:
        schools = db.query(School).filter(School.id.in_(school_ids)).all()
        if not schools:
            return None
        
        for school in schools:
            db.delete(school)
        
        db.commit()
        return len(schools)
    except Exception as e:
        db.rollback()
        return None


def get_schools_analytics(db: Session, page: int = 1, perPage: int = 20, name: Optional[str] = None, state: Optional[str] = None, city: Optional[str] = None, sort: Optional[str] = None):
    """Get schools analytics with enhanced data including active students calculation"""
    try:
        # Start with base query for schools
        query = db.query(School)
        
        # Apply filters
        if name:
            query = query.filter(School.name.ilike(f"%{name}%"))
        
        if state:
            query = query.filter(School.state.ilike(f"%{state}%"))
        
        if city:
            query = query.filter(School.city.ilike(f"%{city}%"))
        
        # Get total count for pagination
        total_count = query.count()
        
        # Apply pagination
        schools = query.offset((page - 1) * perPage).limit(perPage).all()
        
        # Calculate current month for active students calculation
        current_date = datetime.now()
        current_month_start = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        schools_data = []
        for school in schools:
            # Get total students for this school
            total_students = db.query(User).filter(User.school_id == school.id).count()
            
            # Get students count by registration status for this school
            students_by_status = db.query(
                User.registration_status,
                func.count(User.id).label('count')
            ).filter(
                User.school_id == school.id
            ).group_by(
                User.registration_status
            ).all()
            
            # Calculate completed students count and percentage
            completed_count = 0
            for status, count in students_by_status:
                if status == "COMPLETED":
                    completed_count = count
                    break
            
            completed_percentage = (completed_count / total_students * 100) if total_students > 0 else 0
            
            # Calculate active students (students with 3+ reading sessions this month, each over 20 seconds)
            active_students_subquery = db.query(ReadingHistory.user_id).filter(
                ReadingHistory.started_at >= current_month_start,
                ReadingHistory.duration > 20
            ).group_by(
                ReadingHistory.user_id
            ).having(
                func.count(ReadingHistory.id) >= 3
            ).subquery()
            
            active_students_count = db.query(func.count(User.id)).filter(
                User.school_id == school.id,
                User.id.in_(active_students_subquery)
            ).scalar()
            active_percentage = (active_students_count / total_students * 100) if total_students > 0 else 0
            
            # Create school data object
            school_data = {
                "school_name": school.name,
                "id": str(school.id),
                "state": school.state,
                "city": school.city,
                "status": school.status,
                "created_at": school.created_at.isoformat() if school.created_at else None,
                "updated_at": school.updated_at.isoformat() if school.updated_at else None,
                "total_students": total_students,
                "count_of_registered_students": completed_count,
                "percent_of_registered_students": round(completed_percentage, 2),
                "count_of_active_students": active_students_count,
                "percent_of_active_students": round(active_percentage, 2)
            }
            
            schools_data.append(school_data)
        
        # Apply sorting for analytics fields after data processing
        if sort:
            try:
                sort_params = json.loads(sort)
                if isinstance(sort_params, list):
                    for sort_item in sort_params:
                        if isinstance(sort_item, dict):
                            field = sort_item.get("id")
                            desc_order = sort_item.get("desc", False)
                            
                            # Handle analytics field sorting
                            if field == "school_name":
                                schools_data.sort(key=lambda x: x["school_name"], reverse=desc_order)
                            elif field == "total_students":
                                schools_data.sort(key=lambda x: x["total_students"], reverse=desc_order)
                            elif field == "count_of_registered_students":
                                schools_data.sort(key=lambda x: x["count_of_registered_students"], reverse=desc_order)
                            elif field == "percent_of_registered_students":
                                schools_data.sort(key=lambda x: x["percent_of_registered_students"], reverse=desc_order)
                            elif field == "count_of_active_students":
                                schools_data.sort(key=lambda x: x["count_of_active_students"], reverse=desc_order)
                            elif field == "percent_of_active_students":
                                schools_data.sort(key=lambda x: x["percent_of_active_students"], reverse=desc_order)
            except:
                # If sorting fails, keep original order
                pass
        
        return {
            "data": schools_data,
            "total_count": total_count,
            "page": page,
            "perPage": perPage
        }
        
    except Exception as e:
        db.rollback()
        return None


def get_school_by_name(school_name: str, db: Session):
    """Get school by name"""
    try:
        return db.query(School).filter(School.name == school_name).first()
    except Exception as e:
        db.rollback()
        return None


def get_schools_by_status(status: str, db: Session, page: int = 1, limit: int = 20):
    """Get schools filtered by status"""
    try:
        query = db.query(School).filter(School.status == status)
        total_count = query.count()
        schools = query.offset((page - 1) * limit).limit(limit).all()
        
        return {
            "schools": schools,
            "total_count": total_count,
            "page": page,
            "limit": limit,
            "status": status
        }
    except Exception as e:
        db.rollback()
        return None


def get_school_analytics_by_id(school_id: str, db: Session):
    """Get analytics for a single school by ID with enhanced data including active students calculation"""
    try:
        # Get the school
        school = db.query(School).filter(School.id == school_id).first()
        if not school:
            return None

        # Get total students for this school
        total_students = db.query(User).filter(User.school_id == school_id).count()
        
        # Get students count by registration status for this school
        students_by_status = db.query(
            User.registration_status,
            func.count(User.id).label('count')
        ).filter(
            User.school_id == school_id
        ).group_by(
            User.registration_status
        ).all()
        
        # Calculate completed students count and percentage
        completed_count = 0
        for status, count in students_by_status:
            if status == "COMPLETED":
                completed_count = count
                break
        
        completed_percentage = (completed_count / total_students * 100) if total_students > 0 else 0
        
        # Calculate current month for active students calculation
        current_date = datetime.now()
        current_month_start = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate active students (students with 3+ reading sessions this month, each over 20 seconds)
        active_students_subquery = db.query(ReadingHistory.user_id).filter(
            ReadingHistory.started_at >= current_month_start,
            ReadingHistory.duration > 20
        ).group_by(
            ReadingHistory.user_id
        ).having(
            func.count(ReadingHistory.id) >= 3
        ).subquery()
        
        active_students_count = db.query(func.count(User.id)).filter(
            User.school_id == school_id,
            User.id.in_(active_students_subquery)
        ).scalar()
        active_percentage = (active_students_count / total_students * 100) if total_students > 0 else 0
        
        # Convert students_by_status to dictionary format
        status_analytics = {}
        for status, count in students_by_status:
            status_analytics[status] = count

        # Create school data object matching the bulk analytics structure
        school_data = {
            "school_name": school.name,
            "id": str(school.id),
            "state": school.state,
            "city": school.city,
            "status": school.status,
            "created_at": school.created_at.isoformat() if school.created_at else None,
            "updated_at": school.updated_at.isoformat() if school.updated_at else None,
            "total_students": total_students,
            "count_of_registered_students": completed_count,
            "percent_of_registered_students": round(completed_percentage, 2),
            "count_of_active_students": active_students_count,
            "percent_of_active_students": round(active_percentage, 2),
            "students_by_status": status_analytics
        }

        return school_data
    except Exception as e:
        db.rollback()
        return None 