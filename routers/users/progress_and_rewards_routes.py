from fastapi import APIRouter, Request, Response, Depends
from datetime import datetime
from services.books import add_reading_history, get_reading_history_by_user_ic, get_top_readers, get_top_reading_time, get_top_quiz_scores
from sqlalchemy.orm import Session
from database.connection import get_db
from database.models import User, Rewards
from sqlalchemy import desc

router = APIRouter(
    prefix="/api/ebooks",
    tags=["progress_and_rewards"],
)

@router.post("/reading_progress/add")
async def add_reading_progress(request: Request, res: Response = Response(), db: Session = Depends(get_db) ):
    try:
        body = await request.json()

        user_ic = body.get("user_ic")
        book_id = body.get("book_id")
        percentage = body.get("percentage")
        started_time = body.get("started_time")
        duration = body.get("duration")
        score = body.get("score")

        print("==============", user_ic, book_id, percentage, started_time, duration, score)

        user = db.query(User).filter(User.ic_number == user_ic).first()

        if not all([user_ic, book_id, percentage, started_time]):
            res.status_code = 400
            return {"success": False, "error": "Missing required fields"}

        add_reading_history(user.id, book_id, percentage, started_time, duration, score, db)

        return {"success": True, "data": "ok"}
    except Exception as e:
        print(f"Error adding reading progress: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}

@router.get("/reading_progress/{user_ic}")
async def get_reading_progress(user_ic: str, request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        page = int(request.query_params.get('page', 1))
        limit = int(request.query_params.get('limit', 5))
        return {"success": True, "data": get_reading_history_by_user_ic(user_ic, page, limit, db)}
    except Exception as e:
        print(f"Error getting reading progress: {e}")
        return {"success": False, "error": str(e)}

@router.get("/leaderboard/get")
async def get_leaderboard(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        org = request.query_params.get('org', 'read_books')
        group = request.query_params.get('group', 'student')
        limit = int(request.query_params.get('limit', 3))
        
        if(org == 'read_books'):
             leaderboardData = get_top_readers(db, group, limit)
             return {"success": True, "data": leaderboardData}
        elif(org == 'read_time'):
             leaderboardData = get_top_reading_time(db, group, limit)
             return {"success": True, "data": leaderboardData}
        elif (org == 'quiz_scores'):
            leaderboardData = get_top_quiz_scores(db, group, limit)
            return {"success": True, "data": leaderboardData}
        
       
    except Exception as e:
        print(f"Error getting leaderboard: {e}")
        return {"success": False, "error": str(e)}

@router.post("/reward/add")
async def add_reward(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        body = await request.json()

        description = body.get("description")
        badge = body.get("imageUrl")
        title = body.get("name")
        condition = [{"field": key, "limit": value} for key, value in body.get("requirements").items()]
        status = body.get("status")

        new_reward = Rewards(
            title=title,
            description=description,
            badge=badge,
            condition=condition,
            status=status
        )

        db.add(new_reward)
        db.commit()
        db.refresh(new_reward)

        return {"success": True, "data": new_reward}
    except Exception as e:
        print(f"Error adding reward: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}
    
@router.get("/reward/list")
async def getAllRewards(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        page = int(request.query_params.get('page', 1))
        limit = int(request.query_params.get('limit', 10))
        keyword = request.query_params.get('keyword', '').lower()

        # Base query
        query = db.query(Rewards)

        # Apply keyword filter if provided
        if keyword:
            query = query.filter(Rewards.title.ilike(f'%{keyword}%'))

        # Get total count
        total = query.count()

        # Apply pagination
        rewards = query.order_by(desc(Rewards.created_at))\
            .offset((page - 1) * limit)\
            .limit(limit)\
            .all()

        return {
            "success": True, 
            "data": rewards,
            "total": total,
            "page": page,
            "limit": limit
        }
    except Exception as e:
        print(f"Error getting rewards: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}
