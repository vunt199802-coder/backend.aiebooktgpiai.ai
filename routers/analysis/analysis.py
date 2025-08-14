from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime
from sqlalchemy import func
from database.connection import get_db
from database.models import Books, User, ReadingHistory, School
from pydantic import BaseModel

router = APIRouter(
    prefix="/api/analysis",
    tags=["analysis"],
)

class DateRange(BaseModel):
    startDate: datetime
    endDate: datetime

class SchoolStatisticRequest(BaseModel):
    startDate: datetime
    endDate: datetime
    limit: int = 5

@router.post("/statistic")
async def get_statistic(date_range: DateRange, db: Session = Depends(get_db)):
    try:
        #
        totalSchools = db.query(School).count()

        totalUsers = db.query(User).count()
        totalRegisteredUsers = db.query(User).filter(User.registration_status=='COMPLETED').count()
        totalNonRegisteredUsers = db.query(User).filter(User.registration_status!='COMPLETED').count()

        totalBooks = db.query(Books).count()
        totalIndexedBooks = db.query(Books).filter(Books.status == 'indexed').count()
        
        # Filter reading history by date range
        totalReadBooks = db.query(ReadingHistory).filter(
            ReadingHistory.created_at >= date_range.startDate,
            ReadingHistory.created_at <= date_range.endDate
        ).count()
        
        topReadBook = db.query(
            Books.title,
            func.count(ReadingHistory.id).label('read_count')
        ).join(
            ReadingHistory,
            Books.id == ReadingHistory.book_id
        ).filter(
            ReadingHistory.created_at >= date_range.startDate,
            ReadingHistory.created_at <= date_range.endDate
        ).group_by(Books.title)\
         .order_by(func.count(ReadingHistory.id).desc())\
         .first()
        
        topReadLanguageBook = db.query(
            Books.language,
            func.count(ReadingHistory.id).label('read_count')
        ).join(
            ReadingHistory,
            Books.id == ReadingHistory.book_id
        ).filter(
            ReadingHistory.created_at >= date_range.startDate,
            ReadingHistory.created_at <= date_range.endDate
        ).group_by(Books.language)\
         .order_by(func.count(ReadingHistory.id).desc())\
         .first()
        
        return {"success": True, "data": {
            'totalSchools': totalSchools,
            'totalUsers': totalUsers,
            'totalRegisteredUsers': totalRegisteredUsers,
            'totalNonRegisteredUsers': totalNonRegisteredUsers,
            'totalBooks': totalBooks,
            'totalIndexedBooks': totalIndexedBooks,
            'totalReadBooks': totalReadBooks,
            'topReadBook': topReadBook[0] if topReadBook else None,
            'topReadBookCount': topReadBook[1] if topReadBook else None,
            'topReadLanguage': topReadLanguageBook[0] if topReadLanguageBook else None,
            'topReadLanguageCount': topReadLanguageBook[1] if topReadLanguageBook else 0
        }}
    except Exception as e:
        print(f"Error retrieving favorites: {e}")
        return {"success": False, "data": 'Error retrieving favorites'}
    
@router.post("/school-statistic")
async def get_statistic(request: SchoolStatisticRequest, db: Session = Depends(get_db)):
    try:
        school_stats = db.query(
            School.name,
            func.count(User.id).label('student_count')
        ).outerjoin(
            User, School.id == User.school_id
        ).group_by(School.name)\
         .order_by(func.count(User.id).desc())\
         .limit(request.limit)\
         .all()
        
        return {
            "success": True, 
            "data": [
                {"school": school, "count": count} 
                for school, count in school_stats
            ]
        }
    except Exception as e:
        print(f"Error retrieving school statistics: {e}")
        return {"success": False, "data": 'Error retrieving school statistics'}