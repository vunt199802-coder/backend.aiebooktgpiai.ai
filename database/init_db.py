from database.connection import engine
from database.models import Base
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_db():
    """
    Initialize the database by creating all tables defined in the models.
    This function will create any tables that don't exist and won't affect existing tables.
    """
    try:
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully!")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise

def drop_all_tables():
    """
    Drop all tables from the database.
    Use with caution as this will delete all data!
    """
    try:
        logger.warning("Dropping all database tables...")
        Base.metadata.drop_all(bind=engine)
        logger.info("All database tables dropped successfully!")
    except Exception as e:
        logger.error(f"Error dropping database tables: {str(e)}")
        raise

if __name__ == "__main__":
    # When running this file directly, initialize the database
    init_db() 