import os
import jwt
import bcrypt
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import or_
from database.models import User
from schemas.models import UserLoginRequest, UserGoogleLoginRequest, UserSignupRequest, ForgotPasswordRequest, ResetPasswordRequest
from services.brevo_service import BrevoEmailService

import logging

logger = logging.getLogger(__name__)

# JWT Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

reset_tokens = {}

def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def verify_password(password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

def create_jwt_token(user_id: str, email: str) -> str:
    """Create a JWT token for user authentication"""
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)

def verify_jwt_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify and decode a JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.error("JWT token has expired")
        return None
    except jwt.InvalidTokenError:
        logger.error("Invalid JWT token")
        return None

async def send_reset_email(email: str, reset_token: str, user_name: str = None) -> bool:
    """Send password reset email using Brevo API"""
    try:
        return await BrevoEmailService.send_password_reset_email(email, reset_token, user_name)
    except Exception as e:
        logger.error(f"Failed to send reset email: {e}")
        return False

def user_login(login_data: UserLoginRequest, db: Session) -> Dict[str, Any]:
    """User login with email/ic_number and password"""
    try:
        # Find user by email or ic_number
        user = db.query(User).filter(
            or_(User.email == login_data.identifier, User.ic_number == login_data.identifier)
        ).first()
        
        if not user:
            return {
                "success": False,
                "message": "Invalid email/IC number or password. Please check your credentials and try again."
            }
        
        if not user.password_hash:
            return {
                "success": False,
                "message": "Invalid email/IC number or password. Please check your credentials and try again."
            }
        
        # Verify password
        if not verify_password(login_data.password, user.password_hash):
            return {
                "success": False,
                "message": "Invalid email/IC number or password. Please check your credentials and try again."
            }
        
        # Check if user is active
        if user.registration_status != 'active':
            return {
                "success": False,
                "message": "Your account is not active. Please contact administrator to activate your account."
            }
        
        # Create JWT token
        token = create_jwt_token(str(user.id), user.email)
        
        # Update last login (if you have this field)
        user.updated_at = datetime.now()
        db.commit()
        
        return {
            "success": True,
            "message": "Login successful",
            "token": token,
            "user_id": str(user.id),
            "data": {
                "id": str(user.id),
                "email": user.email,
                "ic_number": user.ic_number,
                "name": user.name,
                "registration_status": user.registration_status,
                "avatar_url": user.avatar_url,
                "birth": user.birth,
                "address": user.address,
                "parent": user.parent,
                "school_id": str(user.school_id) if user.school_id else None,
                "rewards": user.rewards,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "updated_at": user.updated_at.isoformat() if user.updated_at else None
            }
        }
        
    except Exception as e:
        logger.error(f"Login error: {e}")
        db.rollback()
        return {
            "success": False,
            "message": "An error occurred during login. Please try again later."
        }

def user_google_login(login_data: UserGoogleLoginRequest, db: Session) -> Dict[str, Any]:
    """User login with email"""
    try:
        # Find user by email or ic_number
        user = db.query(User).filter(
            or_(User.email == login_data.email)
        ).first()
        
        if not user:
            return {
                "success": False,
                "message": "No account found with this email address. Please sign up first."
            }
        
        # Check if user is active
        if user.registration_status != 'active':
            return {
                "success": False,
                "message": "Your account is not active. Please contact administrator to activate your account."
            }
        
        # Create JWT token
        token = create_jwt_token(str(user.id), user.email)
        
        # Update last login (if you have this field)
        user.updated_at = datetime.now()
        db.commit()
        
        return {
            "success": True,
            "message": "Login successful",
            "token": token,
            "user_id": str(user.id),
            "data": {
                "id": str(user.id),
                "email": user.email,
                "ic_number": user.ic_number,
                "name": user.name,
                "registration_status": user.registration_status,
                "avatar_url": user.avatar_url,
                "birth": user.birth,
                "address": user.address,
                "parent": user.parent,
                "school_id": str(user.school_id) if user.school_id else None,
                "rewards": user.rewards,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "updated_at": user.updated_at.isoformat() if user.updated_at else None
            }
        }
        
    except Exception as e:
        logger.error(f"Login error: {e}")
        db.rollback()
        return {
            "success": False,
            "message": "An error occurred during login. Please try again later."
        }


async def user_signup(signup_data: UserSignupRequest, db: Session) -> Dict[str, Any]:
    """User signup with validation for existing ic_number"""
    try:
        # Check if user with ic_number already exists
        existing_user = db.query(User).filter(User.ic_number == signup_data.ic_number).first()
        
        if not existing_user:
            return {
                "success": False,
                "message": "User with this IC number does not exist in the system. Please contact administrator to add your IC number to the system."
            }
        
        # Check if user already has an account
        if existing_user.email or existing_user.password_hash:
            return {
                "success": False,
                "message": "An account with this IC number already exists. Please try logging in instead."
            }
        
        # Check if email is already taken
        email_exists = db.query(User).filter(User.email == signup_data.email).first()
        if email_exists:
            return {
                "success": False,
                "message": "This email address is already registered. Please use a different email or try logging in."
            }
        
        # Hash password
        password_hash = hash_password(signup_data.password)
        
        # Update existing user with new information
        existing_user.email = signup_data.email
        existing_user.phone = signup_data.phone
        existing_user.password_hash = password_hash
        existing_user.registration_status = 'active'
        existing_user.updated_at = datetime.now()
        
        db.commit()
        
        # Create JWT token
        token = create_jwt_token(str(existing_user.id), existing_user.email)
        
        # Send welcome email (async, but don't wait for it to complete)
        try:
            await BrevoEmailService.send_welcome_email(existing_user.email, existing_user.name)
        except Exception as e:
            logger.error(f"Failed to send welcome email: {e}")
        
        return {
            "success": True,
            "message": "Registration successful! Welcome to our platform. A welcome email has been sent to your inbox.",
            "token": token,
            "user_id": str(existing_user.id),
            "data": {
                "id": str(existing_user.id),
                "email": existing_user.email,
                "ic_number": existing_user.ic_number,
                "name": existing_user.name,
                "registration_status": existing_user.registration_status
            }
        }
        
    except Exception as e:
        logger.error(f"Signup error: {e}")
        db.rollback()
        return {
            "success": False,
            "message": "An error occurred during registration. Please try again later or contact support."
        }

async def forgot_password(forgot_data: ForgotPasswordRequest, db: Session) -> Dict[str, Any]:
    """Send password reset email"""
    try:
        # Find user by email
        user = db.query(User).filter(User.email == forgot_data.email).first()
        
        if not user:
            return {
                "success": False,
                "message": "No account found with this email address. Please check your email or contact support."
            }
        
        # Check if user is active
        if user.registration_status != 'active':
            return {
                "success": False,
                "message": "Account is not active. Please contact administrator to activate your account."
            }
        
        # Generate reset token
        reset_token = secrets.token_urlsafe(32)
        
        # Store token with expiration (1 hour)
        reset_tokens[reset_token] = {
            "user_id": str(user.id),
            "email": user.email,
            "expires_at": datetime.utcnow() + timedelta(hours=1)
        }

        # Send reset email
        if await send_reset_email(user.email, reset_token, user.name):
            return {
                "success": True,
                "message": "Password reset email sent successfully. Please check your email inbox and follow the instructions to reset your password."
            }
        else:
            return {
                "success": False,
                "message": "Failed to send password reset email. Please try again later or contact support."
            }
        
    except Exception as e:
        logger.error(f"Forgot password error: {e}")
        return {
            "success": False,
            "message": "An error occurred while processing your request. Please try again later."
        }

async def reset_password(reset_data: ResetPasswordRequest, db: Session) -> Dict[str, Any]:
    """Reset password using token"""
    try:
        # Check if token exists and is valid
        token_data = reset_tokens.get(reset_data.reset_token)
        
        if not token_data:
            return {
                "success": False,
                "message": "Invalid or expired reset token. Please request a new password reset."
            }
        
        # Check if token has expired
        if datetime.utcnow() > token_data["expires_at"]:
            del reset_tokens[reset_data.reset_token]
            return {
                "success": False,
                "message": "Reset token has expired. Please request a new password reset."
            }
        
        # Verify email matches
        if token_data["email"] != reset_data.email:
            return {
                "success": False,
                "message": "Invalid reset token. Please check your email and try again."
            }
        
        # Find user
        user = db.query(User).filter(User.id == token_data["user_id"]).first()
        
        if not user:
            return {
                "success": False,
                "message": "User account not found. Please contact support."
            }
        
        # Hash new password
        password_hash = hash_password(reset_data.new_password)
        
        # Update password
        user.password_hash = password_hash
        user.updated_at = datetime.now()
        
        db.commit()
        
        # Remove used token
        del reset_tokens[reset_data.reset_token]
        
        # Send confirmation email
        try:
            await BrevoEmailService.send_notification_email(
                email=user.email,
                subject="Password Reset Successful",
                message="Your password has been successfully reset. If you did not perform this action, please contact support immediately.",
                user_name=user.name
            )
        except Exception as e:
            logger.error(f"Failed to send password reset confirmation email: {e}")
            # Don't fail the password reset if email fails
        
        return {
            "success": True,
            "message": "Password reset successful. You can now log in with your new password."
        }
        
    except Exception as e:
        logger.error(f"Reset password error: {e}")
        db.rollback()
        return {
            "success": False,
            "message": "An error occurred while resetting your password. Please try again later."
        }

def get_user_profile(user_id: str, db: Session) -> Optional[Dict[str, Any]]:
    """Get user profile by ID"""
    try:
        user = db.query(User).filter(User.id == user_id).first()
        
        if not user:
            return None
        
        return {
            "id": str(user.id),
            "ic_number": user.ic_number,
            "name": user.name,
            "email": user.email,
            "phone": user.phone,
            "avatar_url": user.avatar_url,
            "birth": user.birth,
            "address": user.address,
            "parent": user.parent,
            "school_id": str(user.school_id) if user.school_id else None,
            "registration_status": user.registration_status,
            "rewards": user.rewards,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "updated_at": user.updated_at.isoformat() if user.updated_at else None
        }
        
    except Exception as e:
        logger.error(f"Get user profile error: {e}")
        return None

