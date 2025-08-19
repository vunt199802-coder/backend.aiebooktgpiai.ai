# User Authentication API Documentation

This document describes the user authentication endpoints for the AI Ebook application.

## Base URL

```
/api/user/auth
```

## Endpoints

### 1. User Login

**POST** `/api/user/auth/login`

Authenticate a user with email/IC number and password.

**Request Body:**

```json
{
  "identifier": "user@example.com", // or IC number
  "password": "userpassword"
}
```

**Response (Success - 200):**

```json
{
  "success": true,
  "message": "Login successful",
  "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "user_id": "uuid-here",
  "data": {
    "id": "uuid-here",
    "email": "user@example.com",
    "ic_number": "123456789",
    "name": "User Name",
    "registration_status": "active"
  }
}
```

**Response (Error - 401):**

```json
{
  "success": false,
  "message": "Invalid credentials"
}
```

### 2. User Signup

**POST** `/api/user/auth/signup`

Register a new user account. Only users with existing IC numbers in the system can register.

**Request Body:**

```json
{
  "email": "user@example.com",
  "phone": "+60123456789",
  "password": "userpassword",
  "ic_number": "123456789"
}
```

**Validation Rules:**

- Email must be valid format
- Phone must be valid format (basic validation)
- Password must be at least 8 characters
- IC number must exist in the system
- Email must be unique

**Response (Success - 201):**

```json
{
  "success": true,
  "message": "Registration successful",
  "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "user_id": "uuid-here",
  "data": {
    "id": "uuid-here",
    "email": "user@example.com",
    "ic_number": "123456789",
    "name": "User Name",
    "registration_status": "active"
  }
}
```

**Response (Error - 400):**

```json
{
  "success": false,
  "message": "User with this IC number does not exist in the system. Please contact administrator."
}
```

### 3. Forgot Password

**POST** `/api/user/auth/forgot-password`

Send a password reset token to the user's email.

**Request Body:**

```json
{
  "email": "user@example.com"
}
```

**Response (Success - 200):**

```json
{
  "success": true,
  "message": "Password reset email sent successfully"
}
```

**Response (Error - 400):**

```json
{
  "success": false,
  "message": "User not found"
}
```

### 4. Reset Password

**POST** `/api/user/auth/reset-password`

Reset password using the token received via email.

**Request Body:**

```json
{
  "email": "user@example.com",
  "reset_token": "token-received-via-email",
  "new_password": "newpassword"
}
```

**Response (Success - 200):**

```json
{
  "success": true,
  "message": "Password reset successful"
}
```

**Response (Error - 400):**

```json
{
  "success": false,
  "message": "Invalid or expired reset token"
}
```

### 5. Get User Profile

**GET** `/api/user/auth/profile`

Get the authenticated user's profile information.

**Headers:**

```
Authorization: Bearer <jwt-token>
```

**Response (Success - 200):**

```json
{
  "success": true,
  "message": "Profile retrieved successfully",
  "data": {
    "id": "uuid-here",
    "ic_number": "123456789",
    "name": "User Name",
    "email": "user@example.com",
    "phone": "+60123456789",
    "avatar_url": "https://example.com/avatar.jpg",
    "birth": "1990-01-01",
    "address": "User Address",
    "parent": "Parent Name",
    "school_id": "school-uuid",
    "registration_status": "active",
    "rewards": ["reward1", "reward2"],
    "created_at": "2024-01-01T00:00:00",
    "updated_at": "2024-01-01T00:00:00"
  }
}
```

**Response (Error - 401):**

```json
{
  "success": false,
  "message": "Authorization header required"
}
```

## Authentication

All protected endpoints require a JWT token in the Authorization header:

```
Authorization: Bearer <jwt-token>
```

JWT tokens are valid for 24 hours and contain:

- user_id: User's unique identifier
- email: User's email address
- exp: Expiration timestamp
- iat: Issued at timestamp

## Business Logic

### User Registration Flow

1. Users must have an existing IC number in the system
2. Only one account per IC number is allowed
3. Email addresses must be unique across all users
4. Upon successful registration, user status becomes 'active'

### Password Reset Flow

1. User requests password reset with email
2. System generates a secure token and sends it via email
3. Token expires after 1 hour
4. User uses token to set new password
5. Token is invalidated after use

## Environment Variables

The following environment variables are required:

```env
# JWT Configuration
JWT_SECRET_KEY=your-secret-key-change-in-production

# Email Configuration (for password reset)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-app-password
FROM_EMAIL=noreply@yourapp.com
```

## Database Migration

Run the migration script to add authentication fields to the User table:

```bash
python database/migrate_user_auth.py
```

This will add:

- `phone` column (VARCHAR)
- `password_hash` column (VARCHAR)
- Unique constraints on `ic_number` and `email`
- Default value for `registration_status`

## Error Handling

All endpoints return consistent error responses with:

- `success`: boolean indicating success/failure
- `message`: human-readable error message
- Appropriate HTTP status codes

Common error scenarios:

- 400: Bad Request (validation errors, business logic violations)
- 401: Unauthorized (invalid credentials, missing token)
- 404: Not Found (user not found)
- 500: Internal Server Error (server errors)
