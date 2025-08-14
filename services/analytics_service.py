from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Float, Date
from database.models import ReadingHistory, User
from datetime import datetime, timedelta
from typing import List, Dict, Any


def get_daily_reading_duration_analytics(db: Session, days: int = 130) -> List[Dict[str, Any]]:
    """
    Get daily reading duration analytics for the last N days
    
    Args:
        db: Database session
        days: Number of days to analyze (default: 30)
    
    Returns:
        List of dictionaries containing daily reading statistics
    """
    try:
        # Calculate the start date (N days ago)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days-1)
        
        # Query to get daily reading duration totals
        daily_stats = db.query(
            func.date(ReadingHistory.started_at).label('date'),
            func.sum(ReadingHistory.duration).label('total_duration'),
            func.count(ReadingHistory.id).label('reading_sessions'),
            func.count(func.distinct(ReadingHistory.user_id)).label('active_users')
        ).filter(
            ReadingHistory.started_at >= start_date,
            ReadingHistory.started_at <= end_date + timedelta(days=1)
        ).group_by(
            func.date(ReadingHistory.started_at)
        ).order_by(
            func.date(ReadingHistory.started_at)
        ).all()
        
        # Create a list of dates with data (only dates with activity)
        result = []
        
        for stat in daily_stats:
            if stat.total_duration and stat.total_duration > 0:
                result.append({
                    "date": stat.date.isoformat(),
                    "total_duration_minutes": int(stat.total_duration),
                    "reading_sessions": stat.reading_sessions,
                    "active_users": stat.active_users
                })
        
        return result
        
    except Exception as e:
        db.rollback()
        return None


def get_user_daily_reading_duration(user_id: str, db: Session, days: int = 30) -> List[Dict[str, Any]]:
    """
    Get daily reading duration analytics for a specific user for the last N days
    
    Args:
        user_id: User ID to analyze
        db: Database session
        days: Number of days to analyze (default: 30)
    
    Returns:
        List of dictionaries containing daily reading statistics for the user
    """
    try:
        # Calculate the start date (N days ago)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days-1)
        
        # Query to get daily reading duration for specific user
        daily_stats = db.query(
            func.date(ReadingHistory.started_at).label('date'),
            func.sum(ReadingHistory.duration).label('total_duration'),
            func.count(ReadingHistory.id).label('reading_sessions')
        ).filter(
            ReadingHistory.user_id == user_id,
            ReadingHistory.started_at >= start_date,
            ReadingHistory.started_at <= end_date + timedelta(days=1)
        ).group_by(
            func.date(ReadingHistory.started_at)
        ).order_by(
            func.date(ReadingHistory.started_at)
        ).all()
        
        # Create a list of dates with data (only dates with activity)
        result = []
        
        for stat in daily_stats:
            if stat.total_duration and stat.total_duration > 0:
                result.append({
                    "date": stat.date.isoformat(),
                    "duration_minutes": int(stat.total_duration),
                    "duration_hours": round(stat.total_duration / 60, 2),
                    "reading_sessions": stat.reading_sessions
                })
        
        return result
        
    except Exception as e:
        db.rollback()
        return None


def get_school_daily_reading_duration(school_id: str, db: Session, days: int = 30) -> List[Dict[str, Any]]:
    """
    Get daily reading duration analytics for a specific school for the last N days
    
    Args:
        school_id: School ID to analyze
        db: Database session
        days: Number of days to analyze (default: 30)
    
    Returns:
        List of dictionaries containing daily reading statistics for the school
    """
    try:
        # Calculate the start date (N days ago)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days-1)
        
        # Query to get daily reading duration for specific school
        daily_stats = db.query(
            func.date(ReadingHistory.started_at).label('date'),
            func.sum(ReadingHistory.duration).label('total_duration'),
            func.count(ReadingHistory.id).label('reading_sessions'),
            func.count(func.distinct(ReadingHistory.user_id)).label('active_users')
        ).join(
            User, ReadingHistory.user_id == User.id
        ).filter(
            User.school_id == school_id,
            ReadingHistory.started_at >= start_date,
            ReadingHistory.started_at <= end_date + timedelta(days=1)
        ).group_by(
            func.date(ReadingHistory.started_at)
        ).order_by(
            func.date(ReadingHistory.started_at)
        ).all()
        
        # Create a list of dates with data (only dates with activity)
        result = []
        
        for stat in daily_stats:
            if stat.total_duration and stat.total_duration > 0:
                result.append({
                    "date": stat.date.isoformat(),
                    "total_duration_minutes": int(stat.total_duration),
                    "total_duration_hours": round(stat.total_duration / 60, 2),
                    "reading_sessions": stat.reading_sessions,
                    "active_users": stat.active_users
                })
        
        return result
        
    except Exception as e:
        db.rollback()
        return None
