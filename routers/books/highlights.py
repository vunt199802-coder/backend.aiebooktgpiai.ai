from fastapi import APIRouter, Request, Response, Depends
from openai import OpenAI
from boto3.dynamodb.conditions import Attr
from services.aws_resources import highlights_table
from sqlalchemy.orm import Session
from sqlalchemy import or_
from database.connection import get_db
from database.models import HighLights, User, Books

client = OpenAI()

router = APIRouter(
    prefix="/api/highlights",
    tags=["highlights"],
)

@router.get("/list")
async def getHighlights(
    request: Request, 
    res: Response = Response(),
    db: Session = Depends(get_db)
):
    try:
        # Get query parameters with defaults
        page = int(request.query_params.get('page', 1))
        limit = int(request.query_params.get('limit', 10))
        keyword = request.query_params.get('keyword', '').lower()
        book_id = request.query_params.get('book_id', '')
        user_ic = request.query_params.get('user_id', '')
        isNote = request.query_params.get('notes', 'true') == 'true'
        
        user = db.query(User).filter(User.ic_number == user_ic).first()
        # Start building the query with join
        query = db.query(HighLights, Books).join(Books, HighLights.book_id == Books.id)

        # Apply filters
        if keyword:
            query = query.filter(
                or_(
                    HighLights.text.ilike(f'%{keyword}%'),
                    HighLights.notes.ilike(f'%{keyword}%')
                )
            )
        if book_id:
            query = query.filter(HighLights.book_id == book_id)
        if user.id:
            query = query.filter(HighLights.user_id == user.id)
        if isNote:
            query = query.filter(HighLights.notes != None, HighLights.notes != '')
        else:
            query = query.filter(or_(HighLights.notes == None, HighLights.notes == ''))

        # Get total count before pagination
        total = query.count()

        # Apply pagination
        query = query.order_by(HighLights.created_at.desc())
        query = query.offset((page - 1) * limit).limit(limit)

        # Execute query and get results
        results = query.all()
        
        # Format results to include both highlight and book data
        highlights = []
        for highlight, book in results:
            highlight_dict = {
                "id": highlight.id,
                "book_id": highlight.book_id,
                "user_id": highlight.user_id,
                "text": highlight.text,
                "cfi": highlight.cfi,
                "date": highlight.date,
                "notes": highlight.notes,
                "tag": highlight.tag,
                "range": highlight.range,
                "color": highlight.color,
                "chapter": highlight.chapter,
                "chapter_index": highlight.chapter_index,
                "percentage": highlight.percentage,
                "created_at": highlight.created_at,
                "updated_at": highlight.updated_at,
                "book": {
                    "title": book.title,
                    "thumb_url": book.thumb_url,
                    "language": book.language,
                    "genres": book.genres
                }
            }
            highlights.append(highlight_dict)

        return {
            "success": True,
            "data": highlights,
            "total": total,
            "page": page,
            "limit": limit,
            "has_more": (page * limit) < total
        }

    except Exception as e:
        print(f"Error getting highlights: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}


@router.get("/getByUserIC/{user_ic}")
async def getHighlightsByUserIC(user_ic: str, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.ic_number == user_ic).first()
        highlights = db.query(HighLights).filter(HighLights.user_id == user.id).all()

        return {"success": True, "data": highlights}

    except Exception as e:
        print(f"Error getting highlights by user: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}

@router.post("/add")
async def addHighlight(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        body = await request.json()

        user_ic = body.get("user_ic")
        book_id = body.get("book_id")
        cfi = body.get("cfi")
        chapter = body.get("chapter")
        chapterIndex = body.get("chapterIndex")
        color = body.get("color")
        date = body.get("date")
        notes = body.get("notes")
        percentage = body.get("percentage")
        range = body.get("range")
        tag = body.get("tag")
        text = body.get("text")

        user = db.query(User).filter(User.ic_number == user_ic).first()

        # Store in PostgreSQL
        pg_highlight = HighLights(
            book_id=book_id,
            user_id=user.id,
            text=text,
            cfi=cfi,
            date=date,
            notes=notes,
            tag=tag if isinstance(tag, list) else [tag] if tag else [],
            range=range,
            color=color,
            chapter=chapter,
            chapter_index=chapterIndex,
            percentage=percentage
        )
        db.add(pg_highlight)
        db.commit()
        db.refresh(pg_highlight)

        return {
            "success": True, 
            "data": pg_highlight
        }
    except Exception as e:
        print(f"Error adding highlight: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}
    
@router.delete("/{highlight_id}")
async def deleteHighlight(highlight_id: str, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        # Find and delete the highlight from PostgreSQL
        highlight = db.query(HighLights).filter(HighLights.id == highlight_id).first()
        if not highlight:
            res.status_code = 404
            return {"success": False, "error": "Highlight not found"}

        db.delete(highlight)
        db.commit()

        return {"success": True, "data": highlight}

    except Exception as e:
        print(f"Error deleting highlight: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}

@router.put("/{highlight_id}")
async def updateHighlights(highlight_id: str, body: dict, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        # Find the highlight in PostgreSQL
        highlight = db.query(HighLights).filter(HighLights.id == highlight_id).first()
        if not highlight:
            res.status_code = 404
            return {"success": False, "error": "Highlight not found"}

        # Update highlight fields from body
        for key, value in body.items():
            if hasattr(highlight, key):
                setattr(highlight, key, value)

        # Commit changes to database
        db.commit()
        db.refresh(highlight)

        return {"success": True, "data": highlight}

    except Exception as e:
        print(f"Error updating highlight: {e}")
        res.status_code = 500
        return {"success": False, "error": str(e)}
    
