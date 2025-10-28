import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.join(current_dir, ".."))

from dags.etl.silver import (
    transform_comments,
    transform_likes,
    transform_posts,
    transform_users
)

START_DATE = "2025-01-01"
END_DATE = "2025-01-31"


def test_transform_bronze_users():
    
    try:
        transform_users(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False
        
def test_transform_bronze_posts():
    
    try:
        transform_posts(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False
        
def test_transform_bronze_comments():
    
    try:
        transform_comments(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False

def test_transform_bronze_likes():

    try:
        transform_likes(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False

if __name__ == "__main__":
    test_transform_bronze_users()
    test_transform_bronze_posts()
    test_transform_bronze_comments()
    test_transform_bronze_likes()