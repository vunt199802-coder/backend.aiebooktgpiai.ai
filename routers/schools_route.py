from fastapi import APIRouter, Path, Body, Query, Depends
from sqlalchemy.orm import Session
from dependencies import get_db_session
from controllers.schools_controller import (
    get_all_schools,
    get_school_by_id,
    add_school,
    update_school,
    delete_school,
    delete_bulk_schools,
    get_schools_analytics,
    get_schools_by_status,
    get_school_analytics_by_id,
    get_school_leaderboard_controller
)
from typing import Optional, List

router = APIRouter(prefix="/api/schools", tags=["schools"])

@router.get("/", summary="Get all schools")
def route_get_all_schools(
    page: int = Query(1, ge=1, description="Page number"),
    perPage: int = Query(20, ge=1, le=100, description="Number of items per page"),
    sort: Optional[str] = Query(None, description="Sort parameters as JSON string (e.g., '[{\"id\":\"name\",\"desc\":true}]')"),
    name: Optional[str] = Query(None, description="Filter by school name (partial match)"),
    state: Optional[str] = Query(None, description="Filter by school state (partial match)"),
    city: Optional[str] = Query(None, description="Filter by school city (partial match)"),
    db: Session = Depends(get_db_session)
):
    """Get all schools with their students count
    
    Query Parameters:
    - page: Page number (default: 1)
    - perPage: Items per page (default: 20, max: 100)
    - sort: JSON string for sorting (e.g., '[{"id":"name","desc":true}]')
    - name: Filter schools by name (case-insensitive partial match)
    - state: Filter schools by state (case-insensitive partial match)
    - city: Filter schools by city (case-insensitive partial match)
    - ic_number: Filter schools by user IC number (case-insensitive partial match)
    
    Supported sort fields: name, state, city, status, created_at, students_count
    """
    return get_all_schools(db, page=page, limit=perPage, sort=sort, name=name, state=state, city=city)

@router.get("/by_id/{school_id}", summary="Get one school by ID")
def route_get_school_by_id(
    school_id: str = Path(..., description="School ID"),
    db: Session = Depends(get_db_session)
):
    """Get a single school by its ID with students data"""
    return get_school_by_id(school_id, db)

@router.get("/by_status/{status}", summary="Get schools by status")
def route_get_schools_by_status(
    status: str = Path(..., description="School status to filter by"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Number of items per page"),
    db: Session = Depends(get_db_session)
):
    """Get schools filtered by status"""
    return get_schools_by_status(status, db, page, limit)

@router.post("/", summary="Add a new school")
def route_add_school(
    school_data: dict = Body(..., description="School data (name, state, city)"),
    db: Session = Depends(get_db_session)
):
    """Add a new school to the database"""
    return add_school(school_data, db)

@router.patch("/{school_id}", summary="Update school information")
def route_update_school(
    school_id: str = Path(..., description="School ID"),
    school_data: dict = Body(..., description="Updated school data"),
    db: Session = Depends(get_db_session)
):
    """Update an existing school's information"""
    return update_school(school_id, school_data, db)

@router.delete("/{school_id}", summary="Delete one school")
def route_delete_school(
    school_id: str = Path(..., description="School ID"),
    db: Session = Depends(get_db_session)
):
    """Delete a single school by ID"""
    return delete_school(school_id, db)

@router.delete("/bulk", summary="Delete multiple schools")
def route_delete_bulk_schools(
    school_ids: list = Body(..., description="List of school IDs to delete"),
    db: Session = Depends(get_db_session)
):
    """Delete multiple schools by their IDs"""
    return delete_bulk_schools(school_ids, db)

@router.get("/analytics", summary="Get schools analytics")
def route_get_schools_analytics(
    page: int = Query(1, ge=1, description="Page number"),
    perPage: int = Query(20, ge=1, le=100, description="Number of items per page"),
    name: Optional[str] = Query(None, description="Filter by school name (partial match)"),
    state: Optional[str] = Query(None, description="Filter by school state (partial match)"),
    city: Optional[str] = Query(None, description="Filter by school city (partial match)"),
    sort: Optional[str] = Query(None, description="Sort parameters as JSON string (e.g., '[{\"id\":\"school_name\",\"desc\":true}]')"),
    db: Session = Depends(get_db_session)
):
    """Get schools analytics with enhanced data including active students calculation
    
    Query Parameters:
    - page: Page number (default: 1)
    - perPage: Items per page (default: 20, max: 100)
    - name: Filter schools by name (case-insensitive partial match)
    - state: Filter schools by state (case-insensitive partial match)
    - city: Filter schools by city (case-insensitive partial match)
    - sort: JSON string for sorting (e.g., '[{"id":"school_name","desc":true}]')
    
    Supported sort fields: 
    - school_name: Sort by school name
    - count_of_students_whos_status_is_completed: Sort by completed students count
    - percent_of_students_whos_status_is_completed: Sort by completed students percentage
    - count_of_active_students: Sort by active students count
    - percent_of_active_students: Sort by active students percentage
    """
    return get_schools_analytics(db, page, perPage, name, state, city, sort)

@router.get("/{school_id}/analytics", summary="Get single school analytics")
def route_get_school_analytics_by_id(
    school_id: str = Path(..., description="School ID"),
    db: Session = Depends(get_db_session)
):
    """Get analytics for a single school with enhanced data including active students calculation
    
    Returns the same analytics structure as the bulk analytics endpoint:
    - School information (name, state, city, status, etc.)
    - Total students count
    - Count and percentage of registered students (COMPLETED status)
    - Count and percentage of active students (3+ reading sessions this month, each over 20 seconds)
    - Students count by registration status
    """
    return get_school_analytics_by_id(school_id, db)


@router.get("/{school_id}/leaderboard", summary="Get school leaderboard")
def route_get_school_leaderboard(
    school_id: str = Path(..., description="School ID"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Number of items per page"),
    db: Session = Depends(get_db_session)
):
    """Get leaderboard for a specific school
    
    This endpoint returns a ranked list of students from a specific school based on their total reading scores.
    The scores are grouped by user_id and ranked in descending order.
    
    Query Parameters:
    - page: Page number (default: 1)
    - limit: Items per page (default: 20, max: 100)
    
    Returns:
    - leaderboard: List of ranked students with their scores
    - total_count: Total number of students in the leaderboard
    - page: Current page number
    - limit: Items per page
    - school_id: School ID
    - school_name: School name
    """
    try:
        return get_school_leaderboard_controller(school_id, db, page, limit)
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "message": f"Unexpected error in leaderboard route: {str(e)}",
            "error": "ROUTE_ERROR"
        } 