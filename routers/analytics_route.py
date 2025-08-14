from fastapi import APIRouter, Path, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from dependencies import get_db_session
from controllers.analytics_controller import (
    get_daily_reading_duration_analytics,
    get_user_daily_reading_duration,
    get_school_daily_reading_duration
)

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/reading-duration/daily", summary="Get daily reading duration analytics")
def route_get_daily_reading_duration_analytics(
    days: int = Query(60, ge=1, le=365, description="Number of days to analyze (1-365)"),
    db: Session = Depends(get_db_session)
):
    """Get daily reading duration analytics for the last N days across all users"""
    return get_daily_reading_duration_analytics(db, days)


@router.get("/reading-duration/daily/user/{user_id}", summary="Get user daily reading duration analytics")
def route_get_user_daily_reading_duration(
    user_id: str = Path(..., description="User ID to analyze"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze (1-365)"),
    db: Session = Depends(get_db_session)
):
    """Get daily reading duration analytics for a specific user for the last N days"""
    return get_user_daily_reading_duration(user_id, db, days)


@router.get("/reading-duration/daily/school/{school_id}", summary="Get school daily reading duration analytics")
def route_get_school_daily_reading_duration(
    school_id: str = Path(..., description="School ID to analyze"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze (1-365)"),
    db: Session = Depends(get_db_session)
):
    """Get daily reading duration analytics for a specific school for the last N days"""
    return get_school_daily_reading_duration(school_id, db, days)
