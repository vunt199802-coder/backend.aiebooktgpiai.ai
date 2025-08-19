from fastapi import Request, Response, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from dependencies import get_db_session
from services.user_auth_service import (
    user_login,
    user_google_login,
    user_signup,
    forgot_password,
    reset_password,
    get_user_profile,
    verify_jwt_token
)
from schemas.models import (
    UserLoginRequest,
    UserGoogleLoginRequest,
    UserSignupRequest,
    ForgotPasswordRequest,
    ResetPasswordRequest
)
import logging

logger = logging.getLogger(__name__)

async def user_login_controller(
    login_data: UserLoginRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """User login endpoint"""
    try:
        result = user_login(login_data, db)
        
        if result["success"]:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=result
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=result
            )
            
    except Exception as e:
        logger.error(f"Login controller error: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error"
            }
        )

async def user_google_login_controller(
    login_data: UserGoogleLoginRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """User login endpoint"""
    try:
        result = user_google_login(login_data, db)
        
        if result["success"]:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=result
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=result
            )
            
    except Exception as e:
        logger.error(f"Login controller error: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error"
            }
        )


async def user_signup_controller(
    signup_data: UserSignupRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """User signup endpoint"""
    try:
        result = user_signup(signup_data, db)
        
        if result["success"]:
            return JSONResponse(
                status_code=status.HTTP_201_CREATED,
                content=result
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=result
            )
            
    except Exception as e:
        logger.error(f"Signup controller error: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error"
            }
        )

async def user_forgot_password_controller(
    forgot_data: ForgotPasswordRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """Forgot password endpoint"""
    try:
        result = forgot_password(forgot_data, db)
        
        if result["success"]:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=result
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=result
            )
            
    except Exception as e:
        logger.error(f"Forgot password controller error: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error"
            }
        )

async def user_reset_password_controller(
    reset_data: ResetPasswordRequest,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """Reset password endpoint"""
    try:
        result = reset_password(reset_data, db)
        
        if result["success"]:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=result
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=result
            )
            
    except Exception as e:
        logger.error(f"Reset password controller error: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error"
            }
        )

async def get_user_profile_controller(
    request: Request,
    response: Response,
    db: Session = Depends(get_db_session)
):
    """Get user profile endpoint (requires authentication)"""
    try:
        # Get authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "success": False,
                    "message": "Authorization header required"
                }
            )
        
        # Extract token
        token = auth_header.split(" ")[1]
        
        # Verify token
        payload = verify_jwt_token(token)
        if not payload:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "success": False,
                    "message": "Invalid or expired token"
                }
            )
        
        # Get user profile
        user_profile = get_user_profile(payload["user_id"], db)
        
        if user_profile:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "message": "Profile retrieved successfully",
                    "data": user_profile
                }
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={
                    "success": False,
                    "message": "User not found"
                }
            )
            
    except Exception as e:
        logger.error(f"Get profile controller error: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error"
            }
        )
