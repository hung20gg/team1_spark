from airflow.sdk import DAG, get_current_context
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    migrate_bronze_users,
    migrate_bronze_posts,
    migrate_bronze_comments,
    migrate_bronze_likes,
)

from dags.etl.silver import (
    transform_comments,
    transform_likes,
    transform_posts,
    transform_users,
)

from dags.etl.nlp import (
    transform_silver_keyword,
    transform_silver_sentiment,
)

from dags.etl.gold import (
    create_content_trends,
    create_post_performance,
    create_user_snapshot,
    create_daily_summary,
)

from dags.etl.utils import initialize_spark
from dags.etl.verify import verify_gold_data, verify_bronze_data, verify_silver_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _initialize_spark(app_name: str) -> None:
    initialize_spark(app_name=app_name)

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
    
    # params = {
    #     "start_date": "2025-01-01",
    #     "end_date": "2025-01-31",
    # }

    initialize = PythonOperator(
        task_id='initialize_spark_session',
        python_callable=_initialize_spark,
        op_kwargs={
            "app_name": "social_media_etl_initialization",
        }
    )


    # Bronze -> Silver tasks
    bronze_users = PythonOperator(
        task_id='migrate_bronze_users',
        python_callable=migrate_bronze_users,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    bronze_posts = PythonOperator(
        task_id='migrate_bronze_posts',
        python_callable=migrate_bronze_posts,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    bronze_comments = PythonOperator(
        task_id='migrate_bronze_comments',
        python_callable=migrate_bronze_comments,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    bronze_likes = PythonOperator(
        task_id='migrate_bronze_likes',
        python_callable=migrate_bronze_likes,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    silver_users = PythonOperator(
        task_id='transform_silver_users',
        python_callable=transform_users,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    silver_posts = PythonOperator(
        task_id='transform_silver_posts',
        python_callable=transform_posts,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    silver_comments = PythonOperator(
        task_id='transform_silver_comments',
        python_callable=transform_comments,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    silver_likes = PythonOperator(
        task_id='transform_silver_likes',
        python_callable=transform_likes,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )



    nlp_keyword = PythonOperator(
        task_id='transform_nlp_keyword',
        python_callable=transform_silver_keyword,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    nlp_sentiment = PythonOperator(
        task_id='transform_silver_sentiment',
        python_callable=transform_silver_sentiment,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    # Silver -> Gold tasks
    gold_content_trends = PythonOperator(
        task_id='create_content_trends',
        python_callable=create_content_trends,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    gold_post_performance = PythonOperator(
        task_id='create_post_performance',
        python_callable=create_post_performance,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )   

    gold_user_snapshot = PythonOperator(
        task_id='create_user_snapshot',
        python_callable=create_user_snapshot,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )
    gold_daily_summary = PythonOperator(
        task_id='create_daily_summary',
        python_callable=create_daily_summary,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    verify_bronze = PythonOperator(
        task_id='verify_bronze_data',
        python_callable=verify_bronze_data,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    verify_silver = PythonOperator(
        task_id='verify_silver_data',
        python_callable=verify_silver_data,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )
    
    verify_gold = PythonOperator(
        task_id='verify_gold_data',
        python_callable=verify_gold_data,
        op_kwargs={
            "start_day": "{{ params.start_date }}",
            "end_day": "{{ params.end_date }}",
        }
    )

    bronze_layers = [bronze_users, bronze_posts, bronze_comments, bronze_likes]
    gold_layers = [
        gold_content_trends,
        gold_post_performance,
        gold_user_snapshot,
        gold_daily_summary
    ]
    silver_layers = [silver_users, silver_comments, silver_likes, silver_posts]


    # Define task dependencies
    initialize >> bronze_layers >> verify_bronze >> silver_layers >> verify_silver >> nlp_keyword
    
    nlp_keyword >> nlp_sentiment >> gold_layers >> verify_gold