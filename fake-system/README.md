# AWS PostgreSQL Data Upload

This project contains Python scripts to read CSV data and upload it to an AWS PostgreSQL (RDS) database.

## Files Overview

- **`push_to_aws.py`** - Main script for uploading CSV data to AWS PostgreSQL
- **`analyze_data.py`** - Helper script to analyze CSV files and test database configuration
- **`.env.example`** - Template for database configuration
- **`ddl.sql`** - Database schema definition
- **`data/`** - Directory containing CSV files:
  - `users.csv` (13 MB, ~100K users)
  - `posts.csv` (1.3 GB, millions of posts) 
  - `comments.csv` (553 MB, millions of comments)
  - `likes.csv` (55 MB, millions of likes)

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
- `pandas` - Data manipulation
- `psycopg2-binary` - PostgreSQL adapter
- `python-dotenv` - Environment variable loading
- `boto3` - AWS SDK (if needed for additional AWS services)

### 2. Configure Database Connection

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your AWS RDS PostgreSQL credentials:
```bash
AWS_POSTGRES_HOST=your-rds-endpoint.amazonaws.com
AWS_POSTGRES_PORT=5432
AWS_POSTGRES_DB=your_database_name
AWS_POSTGRES_USER=your_username
AWS_POSTGRES_PASSWORD=your_password
```

### 3. Prepare AWS RDS PostgreSQL Database

Ensure you have:
- An AWS RDS PostgreSQL instance running
- Proper security group settings allowing connections from your IP
- Database user with CREATE and INSERT permissions

## Usage

### Step 1: Analyze Data (Optional)

Run the analysis script to understand your data structure:

```bash
python analyze_data.py
```

This will:
- Show file sizes and column structures
- Display sample data from each CSV file
- Test database configuration

### Step 2: Upload Data

Run the main upload script:

```bash
python push_to_aws.py
```

This will:
1. Test database connection
2. Create tables (if they don't exist)
3. Upload data in the correct order (users → posts → comments → likes)
4. Use chunked processing for large files
5. Verify upload success with row counts

## Features

### Efficient Processing
- **Chunked processing**: Handles large files (GB+) without memory issues
- **Bulk inserts**: Uses PostgreSQL COPY for fast data loading
- **Progress tracking**: Logs progress for large file uploads

### Data Integrity
- **Foreign key constraints**: Maintains referential integrity
- **Upload order**: Processes tables in dependency order
- **Data validation**: Filters columns based on database schema
- **Error handling**: Comprehensive error logging and recovery

### Flexible Configuration
- **Environment variables**: Secure credential management
- **Table clearing**: Option to clear existing data before upload
- **Schema validation**: Ensures data matches expected table structure

## Database Schema

The script creates these tables:

```sql
-- Users table (100K+ records)
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(255),
    email VARCHAR(255),
    password_hash VARCHAR(255),
    full_name VARCHAR(255)
);

-- Posts table (millions of records)
CREATE TABLE posts (
    post_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    content TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Comments table (millions of records)
CREATE TABLE comments (
    comment_id INTEGER PRIMARY KEY,
    post_id INTEGER,
    user_id INTEGER,
    content TEXT,
    FOREIGN KEY (post_id) REFERENCES posts(post_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Likes table (millions of records)
CREATE TABLE likes (
    like_id INTEGER PRIMARY KEY,
    post_id INTEGER,
    user_id INTEGER,
    FOREIGN KEY (post_id) REFERENCES posts(post_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

## Performance Notes

- **Large files**: The posts.csv file is 1.3GB and will take time to upload
- **Memory usage**: Chunked processing keeps memory usage low (~100MB)
- **Network**: Upload speed depends on your internet connection to AWS
- **Database**: Consider using larger RDS instance for faster processing

## Troubleshooting

### Connection Issues
- Check AWS security groups allow connections from your IP
- Verify RDS instance is publicly accessible (if connecting from outside VPC)
- Test credentials and endpoint URL

### Upload Errors  
- Check available disk space on RDS instance
- Monitor RDS performance metrics during upload
- Review logs for specific error messages

### Performance Issues
- Consider using RDS Proxy for connection pooling
- Increase RDS instance size temporarily for large uploads
- Use VPC endpoint for faster AWS network access

## Example Output

```
2025-09-18 10:30:15 - INFO - Database connection successful!
2025-09-18 10:30:16 - INFO - Tables created successfully!
2025-09-18 10:30:16 - INFO - Starting upload of users.csv to table users
2025-09-18 10:30:45 - INFO - Successfully uploaded 100,000 rows to users
2025-09-18 10:30:46 - INFO - Starting upload of posts.csv to table posts
2025-09-18 10:35:20 - INFO - Processed 100 chunks, 1,000,000 total rows for posts
...
2025-09-18 11:15:30 - INFO - Data upload completed successfully!
2025-09-18 11:15:31 - INFO - Table users: 100,000 rows
2025-09-18 11:15:31 - INFO - Table posts: 5,000,000 rows
2025-09-18 11:15:32 - INFO - Table comments: 2,500,000 rows
2025-09-18 11:15:32 - INFO - Table likes: 1,000,000 rows
```
