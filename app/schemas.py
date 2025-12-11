from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional
from datetime import datetime

# User Base
class UserBase(BaseModel):
    email: EmailStr
    username: str = Field(..., min_length=3, max_length=50)
    full_name: Optional[str] = None

# User Create (Registration)
class UserCreate(UserBase):
    password: str = Field(..., min_length=8)
    password_confirm: str
    
    @field_validator('password_confirm')
    def passwords_match(cls, v, values):
        if 'password' in values and v != values['password']:
            raise ValueError('passwords do not match')
        return v
    
    @field_validator('password')
    def password_strength(cls, v):
        if not any(char.isdigit() for char in v):
            raise ValueError('password must contain at least one digit')
        if not any(char.isupper() for char in v):
            raise ValueError('password must contain at least one uppercase letter')
        return v

# User Response
class UserResponse(UserBase):
    id: int
    is_active: bool
    is_verified: bool
    created_at: datetime
    
    class Config:
        from_attributes = True

# Login
class UserLogin(BaseModel):
    username: str
    password: str

# Token
class Token(BaseModel):
    access_token: str
    token_type: str
    user: UserResponse

# Password Reset
class PasswordResetRequest(BaseModel):
    email: EmailStr

class PasswordReset(BaseModel):
    token: str
    new_password: str
    password_confirm: str
    
    @field_validator('password_confirm')
    def passwords_match(cls, v, values):
        if 'new_password' in values and v != values['new_password']:
            raise ValueError('passwords do not match')
        return v