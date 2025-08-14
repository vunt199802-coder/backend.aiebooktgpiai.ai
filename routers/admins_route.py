from fastapi import APIRouter, Path, Body
from controllers.admins_controller import (
    get_all_admins,
    get_admin_by_id,
    add_admin,
    update_admin,
    delete_admin,
    get_admin_by_email,
    get_all_schools,
    count_signin
)

router = APIRouter(prefix="/api/admins", tags=["admins"])

@router.get("/", summary="Get all admins")
def route_get_all_admins():
    return get_all_admins()

@router.get("/by_id/{admin_id}", summary="Get one admin by ID")
def route_get_admin_by_id(admin_id: str = Path(...)):
    return get_admin_by_id(admin_id)

@router.get("/by_email/{email}", summary="Get one admin by email")
def route_get_admin_by_email(email: str = Path(...)):
    return get_admin_by_email(email)

@router.get("/schools", summary="Get schools list")
def route_get_all_schools():
    return get_all_schools()

@router.post("/", summary="Add a new admin")
def route_add_admin(admin_data: dict = Body(...)):
    return add_admin(admin_data)

@router.put("/{admin_id}", summary="Update an admin")
def route_update_admin(admin_id: str = Path(...), admin_data: dict = Body(...)):
    return update_admin(admin_id, admin_data)

@router.delete("/{admin_id}", summary="Delete an admin")
def route_delete_admin(admin_id: str = Path(...)):
    return delete_admin(admin_id)

@router.post("/count-signin", summary="Add a new admin")
def route_count_signin(admin_data: dict = Body(...)):
    return count_signin(admin_data)