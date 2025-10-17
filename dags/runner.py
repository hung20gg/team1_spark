from airflow.sdk import DAG, get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
)
from datetime import datetime, timedelta
import os
import sys

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(DAG_DIR, '..'))

from dags.etl.bronze import (
    transform_bronze_users,
    transform_bronze_posts,
    transform_bronze_comments,
    transform_bronze_likes,
)

from dags.etl.silver import (
    transform_silver_keyword,
)

from dags.etl.gold import (
    create_content_trends,
    create_post_performance,
    create_user_snapshot,
    create_daily_summary,
)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'social_media_etl',
    default_args=default_args,
    description='Bronze -> Silver -> Gold ETL',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
    }
) as dag:
    
    params = {
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
    }


    # Bronze -> Silver tasks
    bronze_users = PythonOperator(
        task_id='transform_bronze_users',
        python_callable=transform_bronze_users,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )

    bronze_posts = PythonOperator(
        task_id='transform_bronze_posts',
        python_callable=transform_bronze_posts,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )

    bronze_comments = PythonOperator(
        task_id='transform_bronze_comments',
        python_callable=transform_bronze_comments,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )

    bronze_likes = PythonOperator(
        task_id='transform_bronze_likes',
        python_callable=transform_bronze_likes,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )

    silver_keyword = PythonOperator(
        task_id='transform_silver_keyword',
        python_callable=transform_silver_keyword,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )

    # Silver -> Gold tasks
    gold_content_trends = PythonOperator(
        task_id='create_content_trends',
        python_callable=create_content_trends,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )

    gold_post_performance = PythonOperator(
        task_id='create_post_performance',
        python_callable=create_post_performance,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )   

    gold_user_snapshot = PythonOperator(
        task_id='create_user_snapshot',
        python_callable=create_user_snapshot,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )
    gold_daily_summary = PythonOperator(
        task_id='create_daily_summary',
        python_callable=create_daily_summary,
        op_kwargs={
            "start_day": params["start_date"],
            "end_day": params["end_date"],
        }
    )


    [bronze_users, bronze_posts, bronze_comments, bronze_likes] >> silver_keyword >> [
        gold_content_trends,
        gold_post_performance,
        gold_user_snapshot,
        gold_daily_summary
    ]