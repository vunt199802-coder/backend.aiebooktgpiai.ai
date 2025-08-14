from fastapi import APIRouter, Path, Body, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional, List
from dependencies import get_db_session
from controllers.users_controller import (
    get_all_users_by_school_id,
    get_user_by_id,
    add_user,
    update_user,
    delete_user,
    delete_bulk_users,
    get_users_by_school_name,
    get_users_by_registration_status,
    get_user_by_ic_number,
    get_users_with_school_id,
    get_user_statistics
)

router = APIRouter(prefix="/api/users", tags=["users"])

@router.get("/by_school/{school_id}", summary="Get all users by school ID")
def route_get_all_users_by_school_id(
    school_id: str = Path(..., description="School ID or 'all' to get all users"),
    page: int = Query(1, ge=1, description="Page number"),
    perPage: int = Query(20, ge=1, le=100, description="Number of items per page"),
    sort: Optional[str] = Query(None, description="Sort parameters as JSON string"),
    name: Optional[str] = Query(None, description="Filter by name"),
    ic_number: Optional[str] = Query(None, description="Filter by IC number"),
    status: Optional[str] = Query(None, description="Filter by status"),
    db: Session = Depends(get_db_session)
):
    """Get all users by school ID. If school_id is 'all', fetch all users"""
    return get_all_users_by_school_id(school_id, db, page, perPage, sort, name, ic_number, status)

@router.get("/by_id/{user_id}", summary="Get one user by ID")
def route_get_user_by_id(
    user_id: str = Path(..., description="User ID"),
    db: Session = Depends(get_db_session)
):
    """Get a single user by its ID"""
    return get_user_by_id(user_id, db)

@router.get("/by_ic/{ic_number}", summary="Get user by IC number")
def route_get_user_by_ic_number(ic_number: str = Path(..., description="IC number")):
    """Get a single user by their IC number"""
    return get_user_by_ic_number(ic_number)

@router.get("/by_school_name/{school_name}", summary="Get users by school name")
def route_get_users_by_school_name(
    school_name: str = Path(..., description="School name to filter by"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Number of items per page")
):
    """Get users filtered by school name"""
    return get_users_by_school_name(school_name, page, limit)

@router.get("/by_status/{status}", summary="Get users by registration status")
def route_get_users_by_registration_status(
    status: str = Path(..., description="Registration status to filter by"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Number of items per page")
):
    """Get users filtered by registration status"""
    return get_users_by_registration_status(status, page, limit)

@router.post("/", summary="Add a new user")
def route_add_user(user_data: dict = Body(..., description="User data (ic_number, name, school, etc.)")):
    """Add a new user to the database"""
    return add_user(user_data)

@router.patch("/{user_id}", summary="Update user information")
def route_update_user(
    user_id: str = Path(..., description="User ID"),
    user_data: dict = Body(..., description="Updated user data")
):
    """Update an existing user's information"""
    return update_user(user_id, user_data)

@router.delete("/{user_id}", summary="Delete one user")
def route_delete_user(user_id: str = Path(..., description="User ID")):
    """Delete a single user by ID"""
    return delete_user(user_id)

@router.delete("/bulk", summary="Delete multiple users")
def route_delete_bulk_users(user_ids: list = Body(..., description="List of user IDs to delete")):
    """Delete multiple users by their IDs"""
    return delete_bulk_users(user_ids)

@router.get("/with_school_id", summary="Get all users with school_id")
def route_get_users_with_school_id(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Number of items per page")
):
    """Get all users with school_id filled based on their school name"""
    return get_users_with_school_id(page, limit)

@router.get("/{user_id}/statistics", summary="Get user reading statistics")
def route_get_user_statistics(
    user_id: str = Path(..., description="User ID"),
    db: Session = Depends(get_db_session)
):
    """Get comprehensive user reading statistics including:
    - Main user information
    - Total read books count
    - Malay/English/Mandarin read books count
    - Total reading duration
    - Read books list (full book objects with all details)
    - Last book read timestamp
    """
    return get_user_statistics(user_id, db) 