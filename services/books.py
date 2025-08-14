from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from sqlalchemy import func, cast
from database.connection import get_db
from database.models import ReadingHistory, ReadingStatistics, User, Books, Rewards
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, ARRAY, DateTime, ForeignKey, JSON, Integer, Float

from .users import get_user_data
from .aws_resources import reading_history_table, dynamodb, reading_statistics_table, ic_numbers_table, rewards_table

def get_reading_history_by_user_ic(user_ic, page, limit, db: Session):
    # Get user_id from user_ic
    user = db.query(User).filter(User.ic_number == user_ic).first()
    if not user:
        return {
            'history': [],
            'books': [],
            'total': 0
        }

    # Query reading history with pagination
    reading_history = db.query(
        ReadingHistory.id,
        ReadingHistory.user_id,
        ReadingHistory.book_id,
        ReadingHistory.duration,
        ReadingHistory.percentage,
        ReadingHistory.score,
        ReadingHistory.started_at,
        Books.id.label('book_id'),
        Books.title,
        Books.thumb_url,
        Books.url,
        Books.language,
        Books.genres,
        Books.file_key,
        Books.status,
    ).join(
        Books, ReadingHistory.book_id == Books.id
    ).filter(
        ReadingHistory.user_id == user.id
    ).order_by(
        ReadingHistory.started_at.desc()
    ).offset(
        (page - 1) * limit
    ).limit(
        limit
    ).all()

    # Get total count for pagination
    total_count = db.query(ReadingHistory)\
        .filter(ReadingHistory.user_id == user.id)\
        .count()

    # Convert results to dictionaries
    history_list = []
    books_list = []
    for record in reading_history:
        history_dict = {
            'id': record.id,
            'user_id': record.user_id,
            'book_id': record.book_id,
            'book_title': record.title,
            'duration': record.duration,
            'percentage': record.percentage,
            'score': record.score,
            'started_at': record.started_at,
        }
        book_dict = {
            'id': record.book_id,
            'title': record.title,
            'thumb_url': record.thumb_url,
            'url': record.url,
            'language': record.language,
            'genres': record.genres,
            'file_key': record.file_key,
            'status': record.status,
        }
        history_list.append(history_dict)
        books_list.append(book_dict)

    return {
        'history': history_list,
        'books': books_list,
        'total': total_count
    }

def get_latest_reading_history_by_user_ic(user_ic):
    
    response = reading_history_table.query(
        KeyConditionExpression=Key('user_ic').eq(user_ic),
        Limit=1,
        ScanIndexForward=False  # This will return items in descending order (newest first)
    )

    items = response.get('Items', [])
    return items[0] if items else None

def add_reading_history(user_id, book_id, percentage, started_time, duration, score, db: Session):
    try:
        # Create new reading history record
        reading_history = ReadingHistory(
            user_id=user_id,
            book_id=book_id,
            duration=int(duration),
            percentage=str(percentage),
            score=str(score),
            started_at=started_time
        )
        
        # Add to database
        db.add(reading_history)
        db.commit()
        
        # Check if user read book in last 24 hours
        is_user_read_book_last_day = False
        last_reading = db.query(ReadingHistory).filter(
            ReadingHistory.user_id == user_id
        ).order_by(ReadingHistory.started_at.desc()).first()
        
        if last_reading:
            current_time = datetime.now(last_reading.started_at.tzinfo)
            last_day = current_time - timedelta(days=1)
            if last_reading.started_at >= last_day:
                is_user_read_book_last_day = True
        
        # Check if this is first time reading this book
        book_reading_count = db.query(ReadingHistory).filter(
            ReadingHistory.user_id == user_id,
            ReadingHistory.book_id == book_id
        ).count()
        
        is_first_time = book_reading_count == 1
        
        if is_first_time:
            update_reading_statistics(user_id, book_id, percentage, started_time, duration, is_user_read_book_last_day, is_first_time)
        else:
            # Calculate total reading times and period for this book
            total_reading_times = book_reading_count
            total_reading_period = db.query(func.sum(ReadingHistory.duration)).filter(
                ReadingHistory.user_id == user_id,
                ReadingHistory.book_id == book_id
            ).scalar() or 0
            
            update_reading_statistics(user_id, book_id, percentage, started_time, duration, is_user_read_book_last_day, is_first_time, total_reading_period, total_reading_times)
            
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_reading_statistics_by_user_ic(user_ic):
    response = reading_statistics_table.get_item(Key={'user_ic': user_ic})
    return response.get('Item', {})

def update_reading_statistics(user_id, book_id, percent, started_time, duration, is_user_read_book_last_day, is_first_time, total_reading_period=0, total_reading_times=0):
    db = next(get_db())
    try:
        # Get existing statistics or create new
        stats = db.query(ReadingStatistics).filter(ReadingStatistics.user_id == user_id).first()
        
        if stats:
            # Update existing statistics
            stats.total_read_books += 1
            stats.total_read_period += int(duration)
            stats.longest_continuous_read_period += (1 if is_user_read_book_last_day else 0)
            
            # Update longest read period for one book
            if total_reading_period > stats.longest_read_period_one_book:
                stats.longest_read_period_one_book = total_reading_period
                stats.longest_read_period_one_book_id = book_id
            
            # Update max read times for one book
            if total_reading_times > stats.max_read_times_one_book:
                stats.max_read_times_one_book = total_reading_times
                stats.max_read_times_one_book_id = book_id
        else:
            # Create new statistics
            stats = ReadingStatistics(
                user_id=user_id,
                total_read_books=1,
                total_read_period=int(duration),
                longest_continuous_read_period=1,
                longest_read_period_one_book=total_reading_period,
                longest_read_period_one_book_id=book_id,
                max_read_times_one_book=total_reading_times,
                max_read_times_one_book_id=book_id
            )
            db.add(stats)
        
        db.commit()
        
        # Check for rewards
        check_user_reward(str(user_id), {
            'total_read_books': stats.total_read_books,
            'total_read_period': stats.total_read_period,
            'longest_continuous_read_period': stats.longest_continuous_read_period,
            'longest_read_period_one_book': stats.longest_read_period_one_book,
            'max_read_times_one_book': stats.max_read_times_one_book
        })
        
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def check_user_reward(user_id, updated_item):
    db = next(get_db())
    try:
        # Get user's unclaimed rewards
        user = db.query(User).filter(user_id == user_id).first()
        if not user:
            return
            
        # Get all rewards
        rewards = db.query(Rewards).all()
        
        for reward in rewards:
            is_matched = True
            for condition in reward.condition:
                field = condition.get('field', '')
                limit = condition.get('limit', 0)
                if float(updated_item.get(field, 0)) < limit:
                    is_matched = False
                    break
                    
            if is_matched and reward.id not in user.rewards:
                # Add reward to user's rewards
                if not user.rewards:
                    user.rewards = []
                user.rewards.append(str(reward.id))
                db.commit()
                
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def get_user_reward(user_ic):
    response = ic_numbers_table.get_item(Key={'icNumber': user_ic})
    user = response.get('Item', {})
    return user.get('rewards', [])

def get_unclaimed_rewards(user_rewards):
    response = rewards_table.scan(FilterExpression=Attr('rewardId').is_in(user_rewards)
)
    return response.get('Items', [])

def add_user_reward(user_ic, reward_id):
    ic_numbers_table.update_item(
        Key={'icNumber': user_ic},
        UpdateExpression='SET rewards = list_append(if_not_exists(rewards, :empty_list), :reward_id)',
        ExpressionAttributeValues={':empty_list': [], ':reward_id': [reward_id]}  # Wrap reward_id in a list
    )

def get_top_readers(db: Session, group='student', limit=3):
    if group == 'school':
        # Get top schools based on total_read_books
        top_schools = db.query(
            School.name,
            func.sum(ReadingStatistics.total_read_books).label('total_books')
        ).join(
            User, School.id == User.school_id
        ).join(
            ReadingStatistics, User.id == ReadingStatistics.user_id
        ).group_by(
            School.name
        ).order_by(
            func.sum(ReadingStatistics.total_read_books).desc()
        ).limit(limit).all()
        
        # Format the results
        result = []
        for school, total_books in top_schools:
            school_data = {
                'name': school,
                'value': total_books
            }
            result.append(school_data)
        return result
    
    # Get top 3 readers based on total_read_books
    top_readers = db.query(
        ReadingStatistics,
        User.ic_number,
        User.name,
        User.avatar_url,
        School.name.label('school_name')
    ).join(
        User, ReadingStatistics.user_id == User.id
    ).outerjoin(
        School, User.school_id == School.id
    ).order_by(
        ReadingStatistics.total_read_books.desc()
    ).limit(limit).all()
    
    # Format the results
    result = []
    for stats, ic_number, name, avatar_url, school in top_readers:
        reader = {
            'user_ic': ic_number,
            'name': name,
            'avatar_url': avatar_url,
            'school': school,
            'value': stats.total_read_books,
            'total_read_period': stats.total_read_period,
            'longest_continuous_read_period': stats.longest_continuous_read_period
        }
        result.append(reader)
    return result

def get_top_reading_time(db: Session, group='student', limit=3):
    if group == 'school':
        # Get top schools based on total_read_period
        top_schools = db.query(
            School.name,
            func.sum(ReadingStatistics.total_read_period).label('read_time')
        ).join(
            User, School.id == User.school_id
        ).join(
            ReadingStatistics, User.id == ReadingStatistics.user_id
        ).group_by(
            School.name
        ).order_by(
            func.sum(ReadingStatistics.total_read_period).desc()
        ).limit(limit).all()
        
        # Format the results
        result = []
        for school, read_time in top_schools:
            school_data = {
                'name': school,
                'value': read_time
            }
            result.append(school_data)
        return result

    # Get top readers based on total_read_period
    top_readers = db.query(
        ReadingStatistics,
        User.ic_number,
        User.name,
        User.avatar_url,
        School.name.label('school_name')
    ).join(
        User, ReadingStatistics.user_id == User.id
    ).outerjoin(
        School, User.school_id == School.id
    ).order_by(
        ReadingStatistics.total_read_period.desc()
    ).limit(limit).all()
    
    # Format the results
    result = []
    for stats, ic_number, name, avatar_url, school in top_readers:
        reader = {
            'user_ic': ic_number,
            'name': name,
            'avatar_url': avatar_url,
            'school': school,
            'total_read_books': stats.total_read_books,
            'value': stats.total_read_period,
            'longest_continuous_read_period': stats.longest_continuous_read_period
        }
        result.append(reader)
    return result

def get_top_quiz_scores(db: Session, group='student', limit=3):
    if group == 'school':
        # Get top schools based on sum of quiz scores
        top_schools = db.query(
            School.name,
            func.sum(cast(ReadingHistory.score, Float)).label('quiz_scores')
        ).join(
            User, School.id == User.school_id
        ).join(
            ReadingHistory, User.id == ReadingHistory.user_id
        ).group_by(
            School.name
        ).order_by(
            func.sum(cast(ReadingHistory.score, Float)).desc()
        ).limit(limit).all()
        
        # Format the results
        result = []
        for school, quiz_scores in top_schools:
            school_data = {
                'name': school,
                'value': float(quiz_scores) if quiz_scores else 0
            }
            result.append(school_data)
        return result

    # Get top users based on sum of quiz scores
    top_scores = db.query(
        User.ic_number,
        User.name,
        User.avatar_url,
        School.name.label('school_name'),
        func.sum(cast(ReadingHistory.score, Float)).label('total_score')
    ).join(
        ReadingHistory, User.id == ReadingHistory.user_id
    ).outerjoin(
        School, User.school_id == School.id
    ).group_by(
        User.id, User.ic_number, User.name, User.avatar_url, School.name
    ).order_by(
        func.sum(cast(ReadingHistory.score, Float)).desc()
    ).limit(limit).all()
    
    # Format the results
    result = []
    for ic_number, name, avatar_url, school, total_score in top_scores:
        scorer = {
            'user_ic': ic_number,
            'name': name,
            'avatar_url': avatar_url,
            'school': school,
            'value': float(total_score) if total_score else 0
        }
        result.append(scorer)
    return result
