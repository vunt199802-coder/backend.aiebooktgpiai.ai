from .users import get_user_data
from .aws_resources import reading_history_table, reading_statistics_table
from sqlalchemy.orm import Session
from database.models import ReadingHistory, User, School
from database.connection import get_db
import uuid

def get_top_readers(count=3):
    response = reading_statistics_table.scan()
    items = response.get('Items', [])
    for item in items:
        try:
            item['total_read_books'] = int(str(item.get('total_read_books', '0')))
        except (ValueError, TypeError):
            item['total_read_books'] = 0
    top_readers = sorted(
        items,
        key=lambda x: x.get('total_read_books', 0),
        reverse=True
    )[:count]
    for reader in top_readers:
        user_ic = reader.get('user_ic', '')
        if user_ic:
            reader.update(get_user_data(user_ic))
    return top_readers

def get_top_reading_time(count=3):
    response = reading_statistics_table.scan()
    items = response.get('Items', [])
    for item in items:
        try:
            item['total_read_period'] = int(str(item.get('total_read_period', '0')))
        except (ValueError, TypeError):
            item['total_read_period'] = 0
    top_reading_time = sorted(
        items,
        key=lambda x: x.get('total_read_period', 0),
        reverse=True
    )[:count]
    for reader in top_reading_time:
        user_ic = reader.get('user_ic', '')
        if user_ic:
            reader.update(get_user_data(user_ic))
    return top_reading_time

def get_top_quiz_scores(count=3):
    response = reading_history_table.scan()
    history_items = response.get('Items', [])
    user_scores = {}
    for item in history_items:
        user_ic = item.get('user_ic', '')
        try:
            score = float(str(item.get('score', '0')))
        except (ValueError, TypeError):
            score = 0
        if user_ic:
            user_scores.setdefault(user_ic, {'user_ic': user_ic, 'score': 0})['score'] += score
    top_quiz_scores = sorted(
        user_scores.values(),
        key=lambda x: x.get('score', 0),
        reverse=True
    )[:count]
    for scorer in top_quiz_scores:
        user_ic = scorer.get('user_ic', '')
        if user_ic:
            scorer.update(get_user_data(user_ic))
    return top_quiz_scores

def get_school_leaderboard(school_id: str, db: Session, page: int = 1, limit: int = 20):
    """Get leaderboard for a specific school by grouping scores by user_id"""
    try:
        try:
            uuid.UUID(school_id)
        except ValueError:
            return {
                "success": False,
                "data": None,
                "message": f"Invalid school_id format: {school_id}. Must be a valid UUID.",
                "error": "INVALID_UUID_FORMAT"
            }
        
        # Validate pagination parameters
        if page < 1:
            return {
                "success": False,
                "data": None,
                "message": "Page number must be greater than 0",
                "error": "INVALID_PAGE_NUMBER"
            }
        
        if limit < 1 or limit > 100:
            return {
                "success": False,
                "data": None,
                "message": "Limit must be between 1 and 100",
                "error": "INVALID_LIMIT"
            }
        
        # Get all users from the school
        try:
            users = db.query(User).filter(User.school_id == school_id).all()
            user_ids = [str(user.id) for user in users]
        except Exception as e:
            return {
                "success": False,
                "data": None,
                "message": f"Database error while fetching users: {str(e)}",
                "error": "DATABASE_ERROR"
            }
        
        if not user_ids:
            return {
                "success": False,
                "data": None,
                "message": f"No users found for school_id: {school_id}",
                "error": "NO_USERS_FOUND"
            }
        
        # Get all reading history for users in this school
        try:
            reading_records = db.query(
                ReadingHistory.user_id,
                ReadingHistory.score,
                User.name,
                User.ic_number,
                User.avatar_url
            ).join(
                User, ReadingHistory.user_id == User.id
            ).filter(
                ReadingHistory.user_id.in_(user_ids)
            ).all()
        except Exception as e:
            return {
                "success": False,
                "data": None,
                "message": f"Database error while fetching reading history: {str(e)}",
                "error": "DATABASE_ERROR"
            }
        
        # Group and calculate scores manually
        user_scores = {}
        for record in reading_records:
            user_id = str(record.user_id)
            if user_id not in user_scores:
                user_scores[user_id] = {
                    "user_id": user_id,
                    "name": record.name,
                    "ic_number": record.ic_number,
                    "avatar_url": record.avatar_url,
                    "total_score": 0.0,
                    "reading_sessions": 0
                }
            
            # Convert score string to float safely
            try:
                score = float(record.score) if record.score else 0.0
            except (ValueError, TypeError):
                score = 0.0
            
            user_scores[user_id]["total_score"] += score
            user_scores[user_id]["reading_sessions"] += 1
        
        # Sort by total score in descending order
        leaderboard_list = sorted(
            user_scores.values(),
            key=lambda x: x["total_score"],
            reverse=True
        )
        
        # Apply pagination
        total_count = len(leaderboard_list)
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_leaderboard = leaderboard_list[start_idx:end_idx]
        
        # Add ranking
        leaderboard = []
        for rank, entry in enumerate(paginated_leaderboard, start=start_idx + 1):
            leaderboard.append({
                "rank": rank,
                "user_id": entry["user_id"],
                "name": entry["name"],
                "ic_number": entry["ic_number"],
                "avatar_url": entry["avatar_url"],
                "total_score": entry["total_score"],
                "reading_sessions": entry["reading_sessions"]
            })
        
        return {
            "success": True,
            "data": {
                "leaderboard": leaderboard,
                "total_count": total_count,
                "page": page,
                "limit": limit,
                "school_id": school_id
            },
            "message": f"Leaderboard fetched successfully for school_id: {school_id}",
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "message": f"Failed to fetch leaderboard for school_id: {school_id}",
            "error": str(e)
        }