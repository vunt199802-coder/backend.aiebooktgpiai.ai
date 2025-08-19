from typing import Optional
from uuid import UUID
from pydantic import BaseModel, EmailStr, validator
import re

class UserLoginRequest(BaseModel):
    identifier: str  # Can be email or ic_number
    password: str

class UserGoogleLoginRequest(BaseModel):
    email: str

class UserSignupRequest(BaseModel):
    email: EmailStr
    phone: str
    password: str
    ic_number: str
    
    @validator('phone')
    def validate_phone(cls, v):
        # Basic phone validation - can be enhanced based on your requirements
        if not re.match(r'^\+?[\d\s\-\(\)]+$', v):
            raise ValueError('Invalid phone number format')
        return v
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ResetPasswordRequest(BaseModel):
    email: EmailStr
    reset_token: str
    new_password: str
    
    @validator('new_password')
    def validate_new_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v

class UserAuthResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None
    token: Optional[str] = None
    user_id: Optional[UUID] = None

class UserProfileResponse(BaseModel):
    id: UUID
    ic_number: Optional[str]
    name: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    avatar_url: Optional[str]
    birth: Optional[str]
    address: Optional[str]
    parent: Optional[str]
    school_id: Optional[UUID]
    registration_status: Optional[str]
    rewards: Optional[list]
    created_at: Optional[str]
    updated_at: Optional[str]

