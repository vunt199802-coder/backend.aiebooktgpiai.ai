from services.analytics_service import (
    get_daily_reading_duration_analytics as db_get_daily_reading_duration_analytics,
    get_user_daily_reading_duration as db_get_user_daily_reading_duration,
    get_school_daily_reading_duration as db_get_school_daily_reading_duration
)
from sqlalchemy.orm import Session
from typing import Optional


def get_daily_reading_duration_analytics(db: Session, days: int = 130):
    """Get daily reading duration analytics for the last N days"""
    result = db_get_daily_reading_duration_analytics(db, days)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": "Failed to fetch daily reading duration analytics",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "analytics": result,
            "total_days": len(result),
            "period": f"Last {days} days",
            "days_with_activity": len(result)
        },
        "message": f"Daily reading duration analytics fetched successfully for the last {days} days",
        "error": None
    }


def get_user_daily_reading_duration(user_id: str, db: Session, days: int = 30):
    """Get daily reading duration analytics for a specific user"""
    result = db_get_user_daily_reading_duration(user_id, db, days)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": f"Failed to fetch daily reading duration analytics for user {user_id}",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "user_id": user_id,
            "analytics": result,
            "total_days": len(result),
            "period": f"Last {days} days",
            "days_with_activity": len(result)
        },
        "message": f"User daily reading duration analytics fetched successfully for the last {days} days",
        "error": None
    }


def get_school_daily_reading_duration(school_id: str, db: Session, days: int = 30):
    """Get daily reading duration analytics for a specific school"""
    result = db_get_school_daily_reading_duration(school_id, db, days)
    
    if result is None:
        return {
            "success": False,
            "data": None,
            "message": f"Failed to fetch daily reading duration analytics for school {school_id}",
            "error": "DATABASE_ERROR"
        }
    
    return {
        "success": True,
        "data": {
            "school_id": school_id,
            "analytics": result,
            "total_days": len(result),
            "period": f"Last {days} days",
            "days_with_activity": len(result)
        },
        "message": f"School daily reading duration analytics fetched successfully for the last {days} days",
        "error": None
    }
