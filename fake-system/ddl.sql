-- PostgreSQL DDL for Social Media Database Tables

-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER,
    username VARCHAR(255),
    email VARCHAR(255),
    password_hash VARCHAR(255),
    full_name VARCHAR(255)
);

-- Posts table  
CREATE TABLE IF NOT EXISTS  posts (
    post_id INTEGER,
    user_id INTEGER,
    content TEXT,
    created_at TIMESTAMP
);

-- Comments table
CREATE TABLE IF NOT EXISTS  comments (
    comment_id INTEGER,
    post_id INTEGER,
    user_id INTEGER,
    content TEXT,
    created_at TIMESTAMP
);

-- Likes table
CREATE TABLE IF NOT EXISTS  likes (
    like_id INTEGER,
    post_id INTEGER,
    user_id INTEGER,
    created_at TIMESTAMP
);


CREATE INDEX idx_posts_created_at ON posts (created_at DESC);
CREATE INDEX idx_comments_created_at ON comments (created_at DESC);
CREATE INDEX idx_likes_created_at ON likes (created_at DESC);

