import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.join(current_dir, ".."))

from dags.etl.gold import (
    create_content_trends,
    create_post_performance,
    create_user_snapshot,
    create_daily_summary,
)

START_DATE = "2025-01-01"
END_DATE = "2025-01-31"


def test_create_content_trends():
    
    try:
        create_content_trends(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False


def test_create_post_performance():

    try:
        create_post_performance(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False


def test_create_user_snapshot():

    try:
        create_user_snapshot(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False


def test_create_daily_summary():

    try:
        create_daily_summary(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False
        
if __name__ == "__main__":
    # test_create_content_trends()
    test_create_post_performance()
    # test_create_user_snapshot()
    # test_create_daily_summary()