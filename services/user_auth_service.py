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
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

logger = logging.getLogger(__name__)

# JWT Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# Email Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = os.getenv("FROM_EMAIL", "noreply@yourapp.com")

# In-memory storage for reset tokens (in production, use Redis or database)
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

def send_reset_email(email: str, reset_token: str) -> bool:
    """Send password reset email"""
    try:
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = email
        msg['Subject'] = "Password Reset Request"
        
        body = f"""
        Hello,
        
        You have requested to reset your password. Please use the following token to reset your password:
        
        Token: {reset_token}
        
        This token will expire in 1 hour.
        
        If you did not request this password reset, please ignore this email.
        
        Best regards,
        Your App Team
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        text = msg.as_string()
        server.sendmail(FROM_EMAIL, email, text)
        server.quit()
        
        return True
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
                "message": "User not found"
            }
        
        if not user.password_hash:
            return {
                "success": False,
                "message": "Invalid credentials"
            }
        
        # Verify password
        if not verify_password(login_data.password, user.password_hash):
            return {
                "success": False,
                "message": "Invalid credentials"
            }
        
        # Check if user is active
        if user.registration_status != 'active':
            return {
                "success": False,
                "message": "Account is not active. Please contact administrator."
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
            "message": "Internal server error"
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
                "message": "User not found"
            }
        
        # Check if user is active
        if user.registration_status != 'active':
            return {
                "success": False,
                "message": "Account is not active. Please contact administrator."
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
            "message": "Internal server error"
        }


def user_signup(signup_data: UserSignupRequest, db: Session) -> Dict[str, Any]:
    """User signup with validation for existing ic_number"""
    try:
        # Check if user with ic_number already exists
        existing_user = db.query(User).filter(User.ic_number == signup_data.ic_number).first()
        
        if not existing_user:
            return {
                "success": False,
                "message": "User with this IC number does not exist in the system. Please contact administrator."
            }
        
        # Check if user already has an account
        if existing_user.email or existing_user.password_hash:
            return {
                "success": False,
                "message": "User with this IC number already has an account"
            }
        
        # Check if email is already taken
        email_exists = db.query(User).filter(User.email == signup_data.email).first()
        if email_exists:
            return {
                "success": False,
                "message": "Email already registered"
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
        
        return {
            "success": True,
            "message": "Registration successful",
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
            "message": "Internal server error"
        }

def forgot_password(forgot_data: ForgotPasswordRequest, db: Session) -> Dict[str, Any]:
    """Send password reset email"""
    try:
        # Find user by email
        user = db.query(User).filter(User.email == forgot_data.email).first()
        
        if not user:
            return {
                "success": False,
                "message": "User not found"
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
        if send_reset_email(user.email, reset_token):
            return {
                "success": True,
                "message": "Password reset email sent successfully"
            }
        else:
            return {
                "success": False,
                "message": "Failed to send reset email"
            }
        
    except Exception as e:
        logger.error(f"Forgot password error: {e}")
        return {
            "success": False,
            "message": "Internal server error"
        }

def reset_password(reset_data: ResetPasswordRequest, db: Session) -> Dict[str, Any]:
    """Reset password using token"""
    try:
        # Check if token exists and is valid
        token_data = reset_tokens.get(reset_data.reset_token)
        
        if not token_data:
            return {
                "success": False,
                "message": "Invalid or expired reset token"
            }
        
        # Check if token has expired
        if datetime.utcnow() > token_data["expires_at"]:
            del reset_tokens[reset_data.reset_token]
            return {
                "success": False,
                "message": "Reset token has expired"
            }
        
        # Verify email matches
        if token_data["email"] != reset_data.email:
            return {
                "success": False,
                "message": "Invalid reset token"
            }
        
        # Find user
        user = db.query(User).filter(User.id == token_data["user_id"]).first()
        
        if not user:
            return {
                "success": False,
                "message": "User not found"
            }
        
        # Hash new password
        password_hash = hash_password(reset_data.new_password)
        
        # Update password
        user.password_hash = password_hash
        user.updated_at = datetime.now()
        
        db.commit()
        
        # Remove used token
        del reset_tokens[reset_data.reset_token]
        
        return {
            "success": True,
            "message": "Password reset successful"
        }
        
    except Exception as e:
        logger.error(f"Reset password error: {e}")
        db.rollback()
        return {
            "success": False,
            "message": "Internal server error"
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
