from fastapi import APIRouter, File, UploadFile, Request, Response, BackgroundTasks, Depends
from openai import OpenAI
from services.aws_resources import S3_CLIENT, region, cognito
from sqlalchemy.orm import Session
from sqlalchemy import func
from database.connection import get_db
from database.models import User, Rewards, ReadingHistory, Books, School
from collections import Counter
from dateutil import parser
import os
from datetime import datetime

client = OpenAI()

BUCKET_NAME = "primary-school-ebook-data"

router = APIRouter(
    prefix="/api/users",
    tags=["users"],
)

# user
@router.get("/list")
async def get_users(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        # response = ic_numbers_table.scan()
        # icNumbers = response.get('Items', [])

        response = cognito.list_users(
            UserPoolId='ap-southeast-2_88E6gZpZz'
        )
        users = response['Users']

        fullUserData = []
        for user in users:
            # Get school name if school_id exists
            school_name = None
            if user.school_id:
                school = db.query(School).filter(School.id == user.school_id).first()
                school_name = school.name if school else None
            
            user_data = {
                "icNumber": user.ic_number,
                "createdAt": user.created_at.isoformat() if user.created_at else '',
                "registrationStatus": user.registration_status,
                "rewards": user.rewards if user.rewards else [],
                "name": user.name,
                "school": school_name,
                "avatar_url": user.avatar_url
            }
            fullUserData.append(user_data)
                
        return {"success": True, "data": fullUserData}
    
    except Exception as e:
        print(f"Error retrieving from database: {e}")
        return {"success": False, "data": 'Error retrieving from database'}

@router.post("/user/add")
async def add_user(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        body = await request.json()
        ic_number = body.get("icNumber")

        existing_user = db.query(User).filter(User.ic_number == ic_number).first()
        if existing_user:
            return {"success": False, "data": 'Duplicate icNumber found'}

        new_user = User(
            ic_number=ic_number,
            registration_status='APPROVED',
            created_at=datetime.utcnow()
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        
        return {"success": True, "data": 'User added successfully'}
    except Exception as e:
        print(f"Error adding user to database: {e}")
        return {"success": False, "data": 'Error adding user to database'}

@router.post("/user/upload_avatar/{user_ic}")
async def upload_avatar(user_ic: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    # generate filename from origin filename and timestamp
    current_timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    filename = file.filename.replace(' ', '-')
    s3_file_key = f"user_avatar/{current_timestamp}_{filename}"

    # save file on local
    try:
        file_data = file.file.read()
        
        S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=s3_file_key,  Body=file_data)
        avatar_url = f"https://{BUCKET_NAME}.s3.{region}.amazonaws.com/{s3_file_key}"
    except Exception as e:
        print(e)
        return {"success": False, "data": 'error occurred while upload to S3 Bucket'}
    finally:
        file.file.close()

    user = db.query(User).filter(User.ic_number == user_ic).first()
    if user:
        user.avatar_url = avatar_url
        db.commit()
        db.refresh(user)

    return {"success": True, "data": avatar_url}

@router.delete("/user/{icNumber}")
async def delete_user(icNumber: str, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.ic_number == icNumber).first()
        if user:
            db.delete(user)
            db.commit()
            
            # Delete the file from S3 bucket if it exists
            if user.avatar_url:
                try:
                    S3_CLIENT.delete_object(Bucket=BUCKET_NAME, Key=icNumber)
                except Exception as s3_error:
                    print(f"Error deleting file from S3: {s3_error}")
            
            return {"success": True, "data": icNumber}
        return {"success": False, "data": 'User not found'}
        
    except Exception as e:
        print(f"Error deleting user from database: {e}")
        return {"success": False, "data": 'Error deleting user from database'}

@router.get("/user/{user_ic}")
async def get_user_by_ic(user_ic: str, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.ic_number == user_ic).first()
        if not user:
            return {"success": False, "data": 'User not found'}
            
        rewards = []
        if user.rewards:
            rewards = db.query(Rewards).filter(Rewards.id.in_(user.rewards)).all()
            
        return {"success": True, "data": {"user": user, "rewards": rewards}}
    except Exception as e:
        print(f"Error retrieving user from database: {e}")
        return {"success": False, "data": 'Error retrieving user from database'}

# reading_progress
# @router.get("/reading_progress")
# async def get_reading_progress(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
#     try:
#         reading_history = db.query(ReadingHistory).order_by(ReadingHistory.started_at.desc()).all()
#         items = []
#         for record in reading_history:
#             items.append({
#                 'user_id': str(record.user_id),
#                 'book_id': str(record.book_id),
#                 'duration': record.duration,
#                 'percent': float(record.percentage),
#                 'started_time': record.started_at.isoformat(),
#                 'created_time': record.created_at.isoformat()
#             })
            
#         return {"success": True, "data": items}
#     except Exception as e:
#         print(f"Error retrieving from database: {e}")
#         return {"success": False, "data": 'Error retrieving from database'}

def analyze_reading_times(reading_times):
    # Convert reading times to datetime objects and extract the hour
    hours = [parser.parse(time).hour for time in reading_times]

    # Count the frequency of each hour
    hour_counts = Counter(hours)

    # Determine the most common reading hours
    most_common_hours = hour_counts.most_common(3)  # Get top 3 most common hours

    # Format the result
    preferred_times = [f"{hour}:00" for hour, _ in most_common_hours]

    return preferred_times