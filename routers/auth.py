from fastapi import APIRouter, Request, Response, status
from controllers.auth_controller import (
    login_controller,
    register_controller,
    forgot_password_controller,
    reset_password_controller
)

router = APIRouter(
    prefix="/api/auth",
    tags=["auth"],
)

@router.post("/login")
async def login(request: Request, response: Response):
    print('login')
    raw_body = await request.body()
    print('Raw body:', raw_body)
    return await login_controller(request, response)

@router.post("/register")
async def register(request: Request, response: Response):
    return await register_controller(request, response)

@router.post("/forgot-password")
async def forgot_password(request: Request, response: Response):
    return await forgot_password_controller(request, response)

@router.post("/reset-password")
async def reset_password(request: Request, response: Response):
    return await reset_password_controller(request, response) 