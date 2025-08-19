from fastapi import APIRouter, Request, Response, Depends, status
from controllers.user_auth_controller import (
    user_login_controller,
    user_google_login_controller,
    user_signup_controller,
    user_forgot_password_controller,
    user_reset_password_controller,
    get_user_profile_controller
)
from schemas.models import (
    UserLoginRequest,
    UserGoogleLoginRequest,
    UserSignupRequest,
    ForgotPasswordRequest,
    ResetPasswordRequest
)
from dependencies import get_db_session
from sqlalchemy.orm import Session

router = APIRouter(
    prefix="/api/user/auth",
    tags=["user-auth"],
)

@router.post("/login", status_code=status.HTTP_200_OK)
async def login(
    login_data: UserLoginRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """
    User login endpoint
    
    Accepts:
    - identifier: email or ic_number
    - password: user password
    
    Returns:
    - JWT token for authentication
    - User information
    """
    return await user_login_controller(login_data, response, db)

@router.post("/google-login", status_code=status.HTTP_200_OK)
async def google_login(
    login_data: UserGoogleLoginRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """
    User login endpoint
    
    Accepts:
    - identifier: email or ic_number
    - password: user password
    
    Returns:
    - JWT token for authentication
    - User information
    """
    return await user_google_login_controller(login_data, response, db)

@router.post("/signup", status_code=status.HTTP_201_CREATED)
async def signup(
    signup_data: UserSignupRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """
    User signup endpoint
    
    Required fields:
    - email: user email
    - phone: user phone number
    - password: user password (min 8 characters)
    - ic_number: user IC number (must exist in system)
    
    Business logic:
    - Only users with existing IC numbers can register
    - Email must be unique
    - IC number must exist in the system
    """
    return await user_signup_controller(signup_data, response, db)

@router.post("/forgot-password", status_code=status.HTTP_200_OK)
async def forgot_password(
    forgot_data: ForgotPasswordRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """
    Forgot password endpoint
    
    Accepts:
    - email: user email
    
    Sends:
    - Password reset token via email
    """
    return await user_forgot_password_controller(forgot_data, response, db)

@router.post("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(
    reset_data: ResetPasswordRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """
    Reset password endpoint
    
    Required fields:
    - email: user email
    - reset_token: token received via email
    - new_password: new password (min 8 characters)
    """
    return await user_reset_password_controller(reset_data, response, db)

@router.get("/profile", status_code=status.HTTP_200_OK)
async def get_profile(
    request: Request,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """
    Get user profile endpoint
    
    Requires:
    - Authorization header with Bearer token
    
    Returns:
    - User profile information
    """
    return await get_user_profile_controller(request, response, db)
