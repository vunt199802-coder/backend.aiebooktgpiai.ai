import requests
import json
import logging
from typing import List, Optional, Dict, Any
from config import settings

logger = logging.getLogger(__name__)


def send_email_brevo(to_email: str, subject: str, text_content: str, html_content: str = None):
    """
    Send an email using Brevo API.
     
    Args:
        to_email: The recipient's email address
        subject: The subject of the email
        text_content: The plain text content of the email
        html_content: The HTML content of the email (optional)
     
    Returns:
        bool: True if email is sent successfully, False otherwise
    """
    url = "https://api.brevo.com/v3/smtp/email"
    
    # Prepare the payload
    payload = {
        "sender": {"name": settings.FROM_NAME, "email": settings.EMAIL_FROM},
        "to": [{"email": to_email}],
        "subject": subject,
        "textContent": text_content,
    }
    
    # Add HTML content if provided
    if html_content:
        payload["htmlContent"] = html_content
    
    headers = {
        "accept": "application/json",
        "api-key": settings.BREVO_API_KEY,
        "content-type": "application/json",
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 201:
            logger.info(f"Email sent successfully to {to_email}")
            return True
        else:
            logger.error(f"Failed to send email to {to_email}. Status: {response.status_code}, Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {to_email}. Error: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}. Error: {e}")
        return False


class BrevoEmailService:
    """Enhanced email service using Brevo API"""
    
    @staticmethod
    async def send_password_reset_email(email: str, reset_token: str, user_name: str = None) -> bool:
        """Send password reset email with enhanced formatting"""
        try:
            subject = "Password Reset Request"
            
            # Plain text version
            text_content = f"""
Hello{user_name if user_name else ""},

You have requested to reset your password. Please use the following token to reset your password:

{reset_token}

Important: This token will expire in 1 hour for security reasons.

Security Notice: If you did not request this password reset, please ignore this email and contact support immediately.

Best regards,
{settings.FROM_NAME}

This is an automated message. Please do not reply to this email.
            """
            
            # HTML version of the email
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Password Reset</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        line-height: 1.6;
                        color: #333;
                        max-width: 600px;
                        margin: 0 auto;
                        padding: 20px;
                    }}
                    .header {{
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 30px;
                        text-align: center;
                        border-radius: 10px 10px 0 0;
                    }}
                    .content {{
                        background: #f9f9f9;
                        padding: 30px;
                        border-radius: 0 0 10px 10px;
                    }}
                    .token-box {{
                        background: #fff;
                        border: 2px solid #667eea;
                        border-radius: 8px;
                        padding: 20px;
                        margin: 20px 0;
                        text-align: center;
                        font-family: 'Courier New', monospace;
                        font-size: 18px;
                        font-weight: bold;
                        color: #667eea;
                    }}
                    .warning {{
                        background: #fff3cd;
                        border: 1px solid #ffeaa7;
                        border-radius: 5px;
                        padding: 15px;
                        margin: 20px 0;
                        color: #856404;
                    }}
                    .footer {{
                        text-align: center;
                        margin-top: 30px;
                        padding-top: 20px;
                        border-top: 1px solid #ddd;
                        color: #666;
                        font-size: 14px;
                    }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>🔐 Password Reset Request</h1>
                </div>
                <div class="content">
                    <p>Hello{f" {user_name}" if user_name else ""},</p>
                    
                    <p>You have requested to reset your password. Please use the following token to reset your password:</p>
                    
                    <div class="token-box">
                        {reset_token}
                    </div>
                    
                    <p><strong>Important:</strong> This token will expire in 1 hour for security reasons.</p>
                    
                    <div class="warning">
                        <strong>⚠️ Security Notice:</strong> If you did not request this password reset, please ignore this email and contact support immediately.
                    </div>
                    
                    <p>Best regards,<br>
                    <strong>{settings.FROM_NAME}</strong></p>
                </div>
                <div class="footer">
                    <p>This is an automated message. Please do not reply to this email.</p>
                </div>
            </body>
            </html>
            """
            
            return send_email_brevo(email, subject, text_content, html_content)
            
        except Exception as e:
            logger.error(f"Failed to send password reset email to {email}: {e}")
            return False
    
    @staticmethod
    async def send_welcome_email(email: str, user_name: str) -> bool:
        """Send welcome email to new users"""
        try:
            subject = "Welcome to Our Platform!"
            
            # Plain text version
            text_content = f"""
Hello {user_name},

Welcome to our platform! We're excited to have you on board.

Your account has been successfully created and is now active. You can start using all the features available to you.

If you have any questions or need assistance, please don't hesitate to contact our support team.

Best regards,
{settings.FROM_NAME}
            """
            
            # HTML version
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Welcome!</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        line-height: 1.6;
                        color: #333;
                        max-width: 600px;
                        margin: 0 auto;
                        padding: 20px;
                    }}
                    .header {{
                        background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
                        color: white;
                        padding: 30px;
                        text-align: center;
                        border-radius: 10px 10px 0 0;
                    }}
                    .content {{
                        background: #f9f9f9;
                        padding: 30px;
                        border-radius: 0 0 10px 10px;
                    }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>🎉 Welcome to Our Platform!</h1>
                </div>
                <div class="content">
                    <p>Hello {user_name},</p>
                    
                    <p>Welcome to our platform! We're excited to have you on board.</p>
                    
                    <p>Your account has been successfully created and is now active. You can start using all the features available to you.</p>
                    
                    <p>If you have any questions or need assistance, please don't hesitate to contact our support team.</p>
                    
                    <p>Best regards,<br>
                    <strong>{settings.FROM_NAME}</strong></p>
                </div>
            </body>
            </html>
            """
            
            return send_email_brevo(email, subject, text_content, html_content)
            
        except Exception as e:
            logger.error(f"Failed to send welcome email to {email}: {e}")
            return False
    
    @staticmethod
    async def send_notification_email(email: str, subject: str, message: str, user_name: str = None) -> bool:
        """Send general notification email"""
        try:
            # Plain text version
            text_content = f"""
Hello{user_name if user_name else ""},

{message}

Best regards,
{settings.FROM_NAME}
            """
            
            # HTML version
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{subject}</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        line-height: 1.6;
                        color: #333;
                        max-width: 600px;
                        margin: 0 auto;
                        padding: 20px;
                    }}
                    .header {{
                        background: linear-gradient(135deg, #2196F3 0%, #1976D2 100%);
                        color: white;
                        padding: 30px;
                        text-align: center;
                        border-radius: 10px 10px 0 0;
                    }}
                    .content {{
                        background: #f9f9f9;
                        padding: 30px;
                        border-radius: 0 0 10px 10px;
                    }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>{subject}</h1>
                </div>
                <div class="content">
                    <p>Hello{f" {user_name}" if user_name else ""},</p>
                    
                    {message}
                    
                    <p>Best regards,<br>
                    <strong>{settings.FROM_NAME}</strong></p>
                </div>
            </body>
            </html>
            """
            
            return send_email_brevo(email, subject, text_content, html_content)
            
        except Exception as e:
            logger.error(f"Failed to send notification email to {email}: {e}")
            return False
    
    @staticmethod
    async def send_bulk_email(emails: List[str], subject: str, message: str) -> Dict[str, Any]:
        """Send bulk emails and return results"""
        results = {
            "success": [],
            "failed": []
        }
        
        for email in emails:
            try:
                # Plain text version
                text_content = f"""
{message}

Best regards,
{settings.FROM_NAME}
                """
                
                # HTML version
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="utf-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>{subject}</title>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            line-height: 1.6;
                            color: #333;
                            max-width: 600px;
                            margin: 0 auto;
                            padding: 20px;
                        }}
                        .header {{
                            background: linear-gradient(135deg, #FF9800 0%, #F57C00 100%);
                            color: white;
                            padding: 30px;
                            text-align: center;
                            border-radius: 10px 10px 0 0;
                        }}
                        .content {{
                            background: #f9f9f9;
                            padding: 30px;
                            border-radius: 0 0 10px 10px;
                        }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>{subject}</h1>
                    </div>
                    <div class="content">
                        {message}
                        
                        <p>Best regards,<br>
                        <strong>{settings.FROM_NAME}</strong></p>
                    </div>
                </body>
                </html>
                """
                
                if send_email_brevo(email, subject, text_content, html_content):
                    results["success"].append(email)
                    logger.info(f"Bulk email sent successfully to {email}")
                else:
                    results["failed"].append({"email": email, "error": "Failed to send email"})
                
            except Exception as e:
                results["failed"].append({"email": email, "error": str(e)})
                logger.error(f"Failed to send bulk email to {email}: {e}")
        
        return results
