import uuid
from datetime import datetime
from decimal import Decimal
import json

from sqlalchemy import Column, String, ARRAY, DateTime, ForeignKey, JSON, Integer, Float
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.types import TypeDecorator, JSON as SQLAlchemyJSON

from database.connection import Base, engine

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

class JSONEncodedDict(TypeDecorator):
    impl = SQLAlchemyJSON

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.loads(json.dumps(value, cls=DecimalEncoder))
        return value

    def process_result_value(self, value, dialect):
        return value

class School(Base):
    __tablename__ = "schools"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    name = Column(String, nullable=False)
    state = Column(String)
    city = Column(String)
    status = Column(String, default='active')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    
    # Relationships
    admins = relationship("Admin", back_populates="school")

class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    ic_number = Column(String)
    avatar_url = Column(String)
    name = Column(String)
    email = Column(String)
    birth = Column(String)
    address = Column(String)
    parent = Column(String)
    school_id = Column(UUID(as_uuid=True), ForeignKey("schools.id"), nullable=True)
    registration_status = Column(String)
    rewards = Column(ARRAY(String))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

class Books(Base):
    __tablename__ = "books"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    title = Column(String)
    file_key = Column(String)
    url = Column(String)
    thumb_url = Column(String)
    thumbnail = Column(String)
    assistant_id = Column(String)
    file_id = Column(String)
    vector_store_id = Column(String)
    language = Column(String)
    genres = Column(ARRAY(String))
    author = Column(String)
    pages = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

class HighLights(Base):
    __tablename__ = "highlights"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    book_id = Column(UUID(as_uuid=True), ForeignKey("books.id"))
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    highlight = Column(String)
    percentage = Column(String)
    text = Column(String)
    cfi = Column(String)
    date = Column(JSONEncodedDict)
    notes = Column(String)
    tag = Column(ARRAY(String))
    range = Column(String)
    color = Column(Integer)
    chapter = Column(Integer)
    chapter_index = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

class Quiz(Base):
    __tablename__ = "quiz"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    book_id = Column(UUID(as_uuid=True), ForeignKey("books.id"))
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    question = Column(String)
    answer = Column(JSONEncodedDict)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

class FavoriteBooks(Base):
    __tablename__ = "favorite_books"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    book_id = Column(UUID(as_uuid=True), ForeignKey("books.id"))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

class Rewards(Base):
    __tablename__ = "rewards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    title = Column(String)
    badge = Column(String)
    condition = Column(JSONEncodedDict)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)


class ReadingHistory(Base):
    __tablename__ = "reading_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    book_id = Column(UUID(as_uuid=True), ForeignKey("books.id"))
    duration = Column(Integer)
    percentage = Column(String)
    score = Column(String)
    started_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

class ReadingStatistics(Base):
    __tablename__ = "reading_statistics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    longest_continuous_read_period = Column(Integer)
    longest_read_period_one_book = Column(Integer)
    longest_read_period_one_book_id = Column(UUID(as_uuid=True), ForeignKey("books.id"))
    max_read_times_one_book = Column(Integer)
    max_read_times_one_book_id = Column(UUID(as_uuid=True), ForeignKey("books.id"))
    total_read_books = Column(Integer)
    total_read_period = Column(Integer)

class Admin(Base):
    __tablename__ = "admins"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    email = Column(String)
    status = Column(String, default='pending')
    role = Column(String, default='school_manager')
    current_role = Column(String)
    createdAt = Column(DateTime(timezone=True), default=datetime.now)
    updatedAt = Column(DateTime(timezone=True), default=datetime.now, onupdate=datetime.now)
    name = Column(String)
    last_login = Column(DateTime(timezone=True))
    school_id = Column(UUID(as_uuid=True), ForeignKey("schools.id"))
    school = relationship("School", back_populates="admins")

Base.metadata.create_all(engine)
