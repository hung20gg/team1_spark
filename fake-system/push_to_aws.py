#!/usr/bin/env python3
"""
Script to read Parquet data and upload to AWS PostgreSQL database.
This script handles Parquet files efficiently using pandas.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from typing import Dict, List, Any
import logging
from contextlib import contextmanager
from io import StringIO
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AWSPostgreSQLUploader:
    """Class to handle uploading Parquet data to AWS PostgreSQL database."""
    
    def __init__(self):
        """Initialize database connection parameters from environment variables."""
        # Load environment variables from .env file
        load_dotenv()
        
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'social_media_db'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'password')
        }
        
        # Data file paths
        self.data_dir = 'data/initial'
        self.parquet_files = {
            'users': os.path.join(self.data_dir, 'users.parquet'),
            'posts': os.path.join(self.data_dir, 'posts.parquet'),
            'comments': os.path.join(self.data_dir, 'comments.parquet'),
            'likes': os.path.join(self.data_dir, 'likes.parquet')
        }
        
        # Table schemas for data validation - updated to match DDL
        self.table_schemas = {
            'users': ['user_id', 'username', 'email', 'password_hash', 'full_name'],
            'posts': ['post_id', 'user_id', 'content', 'created_at'],
            'comments': ['comment_id', 'post_id', 'user_id', 'content', 'created_at'],
            'likes': ['like_id', 'post_id', 'user_id', 'created_at']
        }
        
        # Chunk size for processing large files
        self.chunk_size = 10000
    
    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    logger.info("Database connection successful!")
                    return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_tables(self):
        """Create tables if they don't exist."""
        with open('ddl.sql', 'r') as f:
            ddl_statements = f.read().split(';')
            print(ddl_statements)
        
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for statement in ddl_statements:
                        if statement.strip():
                            cursor.execute(statement)
                    conn.commit()
                    logger.info("Tables created successfully!")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def clear_table(self, table_name: str):
        """Clear all data from a table."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                    conn.commit()
                    logger.info(f"Table {table_name} cleared successfully!")
        except Exception as e:
            logger.error(f"Error clearing table {table_name}: {e}")
            raise
    
    def insert_data_bulk(self, table_name: str, data: pd.DataFrame):
        """Insert data using COPY for better performance."""
        try:
            # Filter columns based on table schema
            schema_columns = self.table_schemas[table_name]
            available_columns = [col for col in schema_columns if col in data.columns]
            filtered_data = data[available_columns].copy()
            # print(available_columns)
            # print(filtered_data.columns)
            # print(filtered_data.head())
            
            # Handle missing columns
            for col in schema_columns:
                if col not in filtered_data.columns:
                    if col in ['label']:  # Skip label columns not in schema
                        continue
                    filtered_data[col] = None
            
            # Reorder columns to match schema
            filtered_data = filtered_data[schema_columns]
            
            # Convert DataFrame to CSV string
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Use execute_values for bulk insert with DataFrame values
                    execute_values(
                        cursor,
                        f"INSERT INTO {table_name} ({', '.join(schema_columns)}) VALUES %s",
                        filtered_data.values.tolist(),
                        template=None,
                        page_size=1000
                    )
                    conn.commit()
                    
            logger.info(f"Inserted {len(filtered_data)} rows into {table_name}")
            
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            raise
    
    def upload_parquet_file(self, table_name: str, file_path: str, clear_first: bool = False):
        """Upload a Parquet file to the specified table."""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return
        
        logger.info(f"Starting upload of {file_path} to table {table_name}")
        
        if clear_first:
            self.clear_table(table_name)
        
        try:
            # Get file size for progress tracking
            file_size = os.path.getsize(file_path)
            logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
            
            # Read Parquet file
            data = pd.read_parquet(file_path)
            
            # Clean data
            data = data.dropna(subset=[data.columns[0]])  # Remove rows where first column (ID) is null
            
            # Process data in chunks to maintain performance and avoid crashes
            total_rows = len(data)
            chunks_processed = 0
            
            for i in range(0, total_rows, self.chunk_size):
                chunk = data.iloc[i:i + self.chunk_size]
                self.insert_data_bulk(table_name, chunk)
                chunks_processed += 1
                
                if chunks_processed % 10 == 0:  # Log progress every 10 chunks
                    logger.info(f"Processed {chunks_processed} chunks, {i + len(chunk)} / {total_rows} rows for {table_name}")
            
            logger.info(f"Successfully uploaded {total_rows} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Error uploading {file_path}: {e}")
            raise
    
    def get_table_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    return count
        except Exception as e:
            logger.error(f"Error getting count for table {table_name}: {e}")
            return -1
    
    def verify_upload(self):
        """Verify the uploaded data by checking row counts."""
        logger.info("Verifying uploaded data...")
        
        for table_name in self.table_schemas.keys():
            count = self.get_table_count(table_name)
            logger.info(f"Table {table_name}: {count} rows")
    
    def upload_all_data(self, clear_tables: bool = False):
        """Upload all Parquet files to their respective tables."""
        # Upload order matters due to foreign key constraints
        upload_order = [ 'posts', 'comments', 'likes']
        
        for table_name in upload_order:
            if table_name in self.parquet_files:
                self.upload_parquet_file(
                    table_name, 
                    self.parquet_files[table_name], 
                    clear_first=clear_tables
                )
        
        self.verify_upload()

def main():
    """Main function to orchestrate the data upload process."""
    uploader = AWSPostgreSQLUploader()
    
    # Test database connection
    if not uploader.test_connection():
        logger.error("Failed to connect to database. Please check your connection parameters.")
        return
    
    try:
        # Create tables
        logger.info("Creating tables...")
        uploader.create_tables()
        
        # Upload all data
        logger.info("Starting data upload...")
        uploader.upload_all_data(clear_tables=True)
        
        logger.info("Data upload completed successfully!")
        
    except Exception as e:
        logger.error(f"Upload process failed: {e}")
        raise

if __name__ == "__main__":
    main()
