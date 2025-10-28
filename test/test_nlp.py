import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.join(current_dir, ".."))

from dags.etl.nlp import (
    transform_silver_keyword,
    transform_silver_sentiment
)

START_DATE = "2025-01-01"
END_DATE = "2025-01-31"


def test_transform_silver_keyword():
    
    try:
        transform_silver_keyword(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False
        
        
def test_transform_silver_sentiment():
    
    try:
        transform_silver_sentiment(START_DATE, END_DATE)
        assert True
    except Exception as e:
        print(f"Error: {e}")
        assert False
        
if __name__ == "__main__":
    test_transform_silver_keyword()
    test_transform_silver_sentiment()