from fastapi import APIRouter, File, UploadFile, Depends, Request, Response, BackgroundTasks
from sqlalchemy.orm import Session
from services.aws_resources import S3_CLIENT, S3_REGION, quizzes_table, rewards_table, highlights_table, reading_statistics_table, reading_history_table, ic_numbers_table, ebook_table, EBOOK_TAG_TABLE, ic_numbers_table, cognito
from boto3.dynamodb.conditions import Attr
from datetime import datetime
import uuid
from database.connection import get_db
from database.models import Books, User, Rewards, FavoriteBooks, Quiz, HighLights, ReadingStatistics, ReadingHistory
from sqlalchemy import text

# S3 Bucket
region = S3_REGION
BUCKET_NAME = "chatbot-voice-clip"

router = APIRouter(
    prefix="/api/ebooks",
    tags=["ebooks"],
)

@router.post("/upload-book")
async def route(file: UploadFile = File(...), res: Response = Response()):
    file_key = f"{int(datetime.utcnow().timestamp())}_{file.filename.replace(' ', '_')}"

    upload_params = {
        'Bucket': BUCKET_NAME,
        'Key': file_key,
        'Body': file.file
    }

    # Upload to S3
    try:
        S3_CLIENT.put_object(**upload_params)
        file_url = f"https://{BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{file_key}"

        return {"file_url": file_url, "filename": file.filename, 'file_key': file_key}
    except Exception as e:
        print(f"Error uploading file: {e}")
        return {"error": str(e)}

@router.get('/list')
async def get_books(
    request: Request,
    res: Response = Response(),
    db: Session = Depends(get_db)
):
    try:
        # Get query parameters
        page = int(request.query_params.get('page', 1))
        limit = int(request.query_params.get('limit', 12))
        keyword = request.query_params.get('keyword', '').lower()
        orderby = request.query_params.get('orderBy', 'upload_time')
        order = request.query_params.get('order', 'desc')
        lang = request.query_params.get('lang', '')
        genres = request.query_params.get('genres', '')
        mode = request.query_params.get('mode', '')
        user_ic = request.query_params.get('user_id', '')
        user = db.query(User).filter(User.ic_number == user_ic).first()
        
        # Start with base query
        query = db.query(Books)


        # Apply filters efficiently
        if keyword:
            # Use ILIKE for case-insensitive search with proper indexing
            query = query.filter(
                (Books.title.ilike(f'%{keyword}%')) |
                (Books.file_key.ilike(f'%{keyword}%'))
            )
        
        if lang:
            # Convert comma-separated languages string to list
            lang_list = [lang.strip() for lang in lang.split(',')]
            query = query.filter(Books.language.in_(lang_list))
            
        if genres:
            # Convert comma-separated genres string to list
            genre_list = [genre.strip() for genre in genres.split(',')]
            # Filter books that have any of the specified genres using PostgreSQL array operator with type casting
            query = query.filter(
                text("books.genres && ARRAY[:genres]::varchar[]")
            ).params(genres=genre_list)
            
        if mode == 'favorite' and user.id:
            # Optimize favorite books query with joins
            query = query.join(
                FavoriteBooks,
                Books.id == FavoriteBooks.book_id
            ).filter(FavoriteBooks.user_id==user.id)

        # Apply sorting efficiently
        sort_column = Books.title
        if order.lower() == 'desc':
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        # Get total count efficiently
        total = query.count()

        # Apply pagination
        query = query.offset((page - 1) * limit).limit(limit)
        
        # Execute query and get results
        books = query.all()
        
        return {
            "success": True,
            "data": books,
            "total": total,
            "page": page,
            "limit": limit,
            "orderby": orderby,
            "order": order
        }
    except ValueError as ve:
        # Handle invalid parameter values
        return {
            "success": False,
            "data": f"Invalid parameter value: {str(ve)}"
        }
    except Exception as e:
        # Log the error and return a generic error message
        print(f"Error retrieving books from database: {e}")
        return {
            "success": False,
            "data": "Error retrieving books from database"
        }

@router.get("/{file_id}")
async def get_book_by_file_key(file_id: str, res: Response = Response(), db : Session=Depends(get_db)):
    try:
        book = db.query(Books).filter(Books.id == file_id).first()
        
        if not book:
            return {"success": False, "data": "Book not found"}
            
        return {"success": True, "data": book}
    except Exception as e:
        print(f"Error retrieving from database: {e}")
        return {"success": False, "data": 'Error retrieving from database'}

@router.post("/add-favorite")
async def add_favorite(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        body = await request.json()
        user_ic = body.get("user_id")
        book_id = body.get("book_id")

        user = db.query(User).filter(User.ic_number == user_ic).first()
        
        # Check if the favorite already exists
        existing_favorite = db.query(FavoriteBooks).filter(
            FavoriteBooks.user_id == user.id,
            FavoriteBooks.book_id == book_id
        ).first()
        
        if existing_favorite:
            return {"success": False, "data": "Book is already in favorites"}
        
        # Create new favorite entry
        new_favorite = FavoriteBooks(
            user_id=user.id,
            book_id=book_id
        )
        
        # Add to database
        db.add(new_favorite)
        db.commit()
        
        return {"success": True, "data": "Book added to favorites"}
        
    except Exception as e:
        print(f"Error adding to favorites: {e}")
        return {"success": False, "data": str(e)}

@router.post("/remove-favorite")
async def remove_favorite(request: Request, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        body = await request.json()
        user_ic = body.get("user_id")
        book_id = body.get("book_id")
        user = db.query(User).filter(User.ic_number == user_ic).first()
        
        # Find and delete the favorite entry
        favorite = db.query(FavoriteBooks).filter(
            FavoriteBooks.user_id == user.id,
            FavoriteBooks.book_id == book_id
        ).first()
        
        if not favorite:
            return {"success": False, "data": "Book not found in favorites"}
            
        # Delete the favorite entry
        db.delete(favorite)
        db.commit()
        
        return {"success": True, "data": "Book removed from favorites"}
    
    except Exception as e:
        print(f"Error removing from favorites: {e}")
        return {"success": False, "data": str(e)}

@router.get("/favorites/{user_ic}")
async def get_favorites(user_ic: str, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.ic_number == user_ic).first()
        favorites = db.query(Books.id).filter(
            FavoriteBooks.user_id == user.id
        ).join(
            FavoriteBooks,
            Books.id == FavoriteBooks.book_id
        ).all()
        
        # Convert list of tuples to list of IDs
        favorite_ids = [book_id[0] for book_id in favorites]
        
        return {"success": True, "data": favorite_ids}
    except Exception as e:
        print(f"Error retrieving favorites: {e}")
        return {"success": False, "data": 'Error retrieving favorites'}

@router.delete("/{file_id}")
async def delete_ebook(file_id: str, res: Response = Response(), db: Session = Depends(get_db)):
    try:
        book = db.query(Books).filter(Books.id == file_id).first()
        
        if not book:
            return {"success": False, "message": f"Book with file_key '{file_id}' not found"}
            
        # Update the status to 'deleted'
        book.status = 'deleted'
        book.updated_at = datetime.utcnow()
        
        # Commit the changes
        db.commit()
        
        return {"success": True, "message": f"Book '{file_id}' marked as deleted"}
    except Exception as e:
        print(f"Error updating book status: {e}")
        return {"success": False, "message": f"Error updating book status: {str(e)}"}

@router.post('/migrate-books')
async def migrate_db():
    try:
        # Scan all books from DynamoDB
        response = ic_numbers_table.scan()
        books = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = ebook_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            books.extend(response.get('Items', []))
        
        # Get database session
        db = next(get_db())
        
        # Migrate each book
        migrated_count = 0
        for book in books:
            try:
                # _genres = book.get('genres', [])
                # Extract genre values from DynamoDB format
                # genres = [genre['S'] for genre in _genres] if isinstance(_genres, list) else []
                
                # Create new book record
                new_book = Books(
                    title=book.get('title'),
                    file_key=book.get('file_key'),
                    url=book.get('url'),
                    thumb_url=book.get('thumb_url'),
                    thumbnail=book.get('thumbnail'),
                    assistant_id=book.get('assistant_id'),
                    file_id=book.get('file_id'),
                    vector_store_id=book.get('vector_store_id'),
                    language=book.get('language'),
                    genres=book.get('genres', []),
                    author=book.get('author'),
                    pages=book.get('pages'),
                    status=book.get('status', 'active')
                )
                
                # Add to database
                db.add(new_book)
                migrated_count += 1
                
            except Exception as e:
                print(f"Error migrating book {book.get('file_key')}: {str(e)}")
                continue
        
        # Commit changes
        db.commit()
        
        return {
            'status': 'success',
            'message': f'Successfully migrated {migrated_count} books to the database'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error during migration: {str(e)}'
        }

@router.post('/migrate-users')
async def migrate_users():
    try:
        # Scan all users from DynamoDB
        response = ic_numbers_table.scan()
        users = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = ic_numbers_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            users.extend(response.get('Items', []))
        
        # Get database session
        db = next(get_db())
        
        # Get all rewards from database to create title to id mapping
        rewards = db.query(Rewards).all()
        rewardId_to_id = {reward.title.lower().replace(' ', '_'): str(reward.id) for reward in rewards}
        
        # Migrate each user
        migrated_count = 0
        for user in users:
            try:
                # Convert reward titles to reward IDs
                reward_titles = user.get('rewards', [])
                reward_ids = [rewardId_to_id[title.lower()] for title in reward_titles if title.lower() in rewardId_to_id]
                # # Create new user record
                new_user = User(
                    ic_number=user.get('icNumber'),
                    avatar_url=user.get('avatar_url'),
                    name=user.get('name'),
                    school=user.get('school'),
                    registration_status=user.get('registrationStatus', 'pending'),
                    rewards=reward_ids,
                )

                # Add to database
                db.add(new_user)
                migrated_count += 1
                
            except Exception as e:
                print(f"Error migrating user {user.get('icNumber')}: {str(e)}")
                continue
        
        # Commit changes
        db.commit()
        
        return {
            'status': 'success',
            'message': f'Successfully migrated {migrated_count} users to the database'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error during migration: {str(e)}'
        }

@router.post('/migrate-rewards')
async def migrate_rewards():
    try:
        # Scan all rewards from DynamoDB
        response = rewards_table.scan()
        rewards = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = rewards_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            rewards.extend(response.get('Items', []))
        
        # Get database session
        db = next(get_db())
        
        # Migrate each reward
        migrated_count = 0
        for reward in rewards:
            try:
                # Create new reward record
                new_reward = Rewards(
                    title=reward.get('title'),
                    badge=reward.get('badge'),
                    condition=reward.get('condition', {}),
                    status=reward.get('status', 'active')
                )
                
                # Add to database
                db.add(new_reward)
                migrated_count += 1
                
            except Exception as e:
                print(f"Error migrating reward {reward.get('rewardId')}: {str(e)}")
                continue
        
        # Commit changes
        db.commit()
        
        return {
            'status': 'success',
            'message': f'Successfully migrated {migrated_count} rewards to the database'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error during migration: {str(e)}'
        }

@router.post('/migrate-quizzes')
async def migrate_quizzes():
    try:
        # Scan all quizzes from DynamoDB
        response = quizzes_table.scan()
        quizzes = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = quizzes_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            quizzes.extend(response.get('Items', []))
        
        # Get database session
        db = next(get_db())
        
        # Get all books from database to create file_key to id mapping
        books = db.query(Books).all()
        file_key_to_id = {book.file_key: str(book.id) for book in books}
        
        # Migrate each quiz
        migrated_count = 0
        for quiz in quizzes:
            try:
                # Get the book_id from the file_key mapping
                file_key = quiz.get('file_key')
                if file_key.startswith('compressed/'):
                    file_key = file_key[len('compressed/'):]  # Remove the prefix
                book_id = file_key_to_id.get(file_key.replace(' ', '_'))
                if not book_id:
                    print(f"Book not found for file_key: {quiz.get('file_key')}")
                    continue
                
                # Create new quiz record
                new_quiz = Quiz(
                    book_id=book_id,
                    question=quiz.get('question'),
                    answer=quiz.get('answer'),
                    created_at=datetime.fromisoformat(quiz.get('created_time')),
                    updated_at=datetime.fromisoformat(quiz.get('created_time'))
                )
                
                # Add to database
                db.add(new_quiz)
                migrated_count += 1
                
            except Exception as e:
                print(f"Error migrating quiz {quiz.get('quiz_id')}: {str(e)}")
                continue
        
        # Commit changes
        db.commit()
        
        return {
            'status': 'success',
            'message': f'Successfully migrated {migrated_count} quizzes to the database'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error during migration: {str(e)}'
        }

@router.post('/migrate-reading-history')
async def migrate_reading_history():
    try:
        # Scan all reading history from DynamoDB
        response = reading_history_table.scan()
        history_items = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = reading_history_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            history_items.extend(response.get('Items', []))
        
        # Get database session
        db = next(get_db())
        
        # Get all users and books from database to create mappings
        users = db.query(User).all()
        books = db.query(Books).all()
        
        user_ic_to_id = {user.ic_number: str(user.id) for user in users}
        book_file_key_to_id = {book.file_key: str(book.id) for book in books}
        
        # Migrate each reading history item
        migrated_count = 0
        for item in history_items:
            try:
                user_id = user_ic_to_id.get(item.get('user_ic'))
                book_id = book_file_key_to_id.get(item.get('book_file_key'))
                
                if not user_id or not book_id:
                    print(f"Skipping reading history item - User or book not found: {item}")
                    continue
                
                # Create new reading history record
                new_history = ReadingHistory(
                    user_id=user_id,
                    book_id=book_id,
                    duration=int(item.get('duration', 0)),
                    percentage=item.get('percent', 0),
                    score=item.get('score', 0),
                    started_at=datetime.fromisoformat(item.get('started_time').replace('Z', '+00:00')),
                )
                
                # Add to database
                db.add(new_history)
                migrated_count += 1
                
            except Exception as e:
                print(f"Error migrating reading history item: {str(e)}")
                continue
        
        # Commit changes
        db.commit()
        
        return {
            'status': 'success',
            'message': f'Successfully migrated {migrated_count} reading history items to the database'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error during migration: {str(e)}'
        }

@router.post('/migrate-reading-statistics')
async def migrate_reading_statistics():
    try:
        # Scan all reading statistics from DynamoDB
        response = reading_statistics_table.scan()
        stats_items = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = reading_statistics_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            stats_items.extend(response.get('Items', []))
        
        # Get database session
        db = next(get_db())
        
        # Get all users and books from database to create mappings
        users = db.query(User).all()
        books = db.query(Books).all()
        
        user_ic_to_id = {user.ic_number: str(user.id) for user in users}
        book_file_key_to_id = {book.file_key: str(book.id) for book in books}
        
        # Migrate each reading statistics item
        migrated_count = 0
        for item in stats_items:
            try:
                user_id = user_ic_to_id.get(item.get('user_ic'))
                longest_read_book_id = book_file_key_to_id.get(item.get('longest_read_period_one_book_key'))
                max_read_times_book_id = book_file_key_to_id.get(item.get('max_read_times_one_book_key'))
                
                if not user_id:
                    print(f"Skipping reading statistics item - User not found: {item}")
                    continue
                
                # Create new reading statistics record
                new_stats = ReadingStatistics(
                    user_id=user_id,
                    longest_continuous_read_period=int(item.get('longest_continuous_read_period', 0)),
                    longest_read_period_one_book=int(item.get('longest_read_period_one_book', 0)),
                    longest_read_period_one_book_id=longest_read_book_id,
                    max_read_times_one_book=int(item.get('max_read_times_one_book', 0)),
                    max_read_times_one_book_id=max_read_times_book_id,
                    total_read_books=int(item.get('total_read_books', 0)),
                    total_read_period=int(item.get('total_read_period', 0))
                )
                
                # Add to database
                db.add(new_stats)
                migrated_count += 1
                
            except Exception as e:
                print(f"Error migrating reading statistics item: {str(e)}")
                continue
        
        # Commit changes
        db.commit()
        
        return {
            'status': 'success',
            'message': f'Successfully migrated {migrated_count} reading statistics items to the database'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error during migration: {str(e)}'
        }

@router.post('/migrate-highlights')
async def migrate_highlights():
    try:
        # Scan all highlights from DynamoDB
        response = highlights_table.scan()
        highlight_items = response.get('Items', [])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = highlights_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            highlight_items.extend(response.get('Items', []))
        
        # Get database session
        db = next(get_db())
        
        # Get all users and books from database to create mappings
        users = db.query(User).all()
        books = db.query(Books).all()
        
        user_ic_to_id = {user.ic_number: str(user.id) for user in users}
        book_file_key_to_id = {book.file_key: str(book.id) for book in books}
        
        # Migrate each highlight item
        migrated_count = 0
        for item in highlight_items:
            try:
                user_id = user_ic_to_id.get(item.get('user_ic'))
                book_id = book_file_key_to_id.get(item.get('file_key'))
                
                if not user_id or not book_id:
                    print(f"Skipping highlight item - User or book not found: {item}")
                    continue
                
                # Create new highlight record
                new_highlight = HighLights(
                    user_id=user_id,
                    book_id=book_id,
                    highlight=item.get('highlight'),
                    text=item.get('text'),
                    cfi=item.get('cfi'),
                    date=item.get('date'),
                    tag=item.get('tag'),
                    notes=item.get('notes'),
                    range=item.get('range'),
                    color=int(item.get('color', 0)),
                    chapter=int(item.get('chapter', 0)),
                    chapter_index=int(item.get('chapter_index', 0))
                )
                
                # Add to database
                db.add(new_highlight)
                migrated_count += 1
                
            except Exception as e:
                print(f"Error migrating highlight item: {str(e)}")
                continue
        
        # Commit changes
        db.commit()
        
        return {
            'status': 'success',
            'message': f'Successfully migrated {migrated_count} highlight items to the database'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error during migration: {str(e)}'
        }
    
@router.post('/migrate-cognito-users')
async def update_user_table(db: Session = Depends(get_db)):
    try:
        # Get all users from Cognito pool with pagination
        cognito_users = []
        pagination_token = None
        
        while True:
            # Prepare parameters for list_users
            params = {
                'UserPoolId': 'ap-southeast-2_88E6gZpZz',
                'Limit': 60  # Maximum allowed by Cognito
            }
            
            # Add pagination token if we have one
            if pagination_token:
                params['PaginationToken'] = pagination_token
                
            # Get users for current page
            response = cognito.list_users(**params)
            
            # Add users from current page to our list
            cognito_users.extend(response['Users'])
            
            # Check if there are more pages
            if 'PaginationToken' not in response:
                break
                
            pagination_token = response['PaginationToken']

        print('Total users count:', len(cognito_users), '\n')
        
        # Get all users from database
        db_users = db.query(User).all()
        
        # Create a mapping of ic_number to user for quick lookup
        db_user_map = {user.ic_number: user for user in db_users}
        
        updated_count = 0
        for cognito_user in cognito_users:
            print('===',cognito_user, '\n')
            # Get user attributes
            ic_number = cognito_user.get('Username')
            
            if ic_number and ic_number in db_user_map:
                user = db_user_map[ic_number]
                if user.registration_status != 'COMPLETED':
                    user.registration_status = 'COMPLETED'
                    updated_count += 1
        
        # Commit changes
        db.commit()
        
        return {
            'success': True,
            'total_users': len(cognito_users)
            # 'message': f'Successfully updated {updated_count} users registration status'
        }
        
    except Exception as e:
        print(f"Error updating user registration status: {e}")
        return {
            'success': False,
            'message': f'Error updating user registration status: {str(e)}'
        }

