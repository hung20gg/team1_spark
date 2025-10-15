from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

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
    # schedule_interval='@daily',
    catchup=False,
) as dag:

    # Bronze -> Silver tasks
    bronze_users = SparkSubmitOperator(
        task_id='transform_bronze_users',
        application=f'{DAG_DIR}/bronze/bronze_users.py',
        conn_id='spark_default',
    )

    bronze_posts = SparkSubmitOperator(
        task_id='transform_bronze_posts',
        application=f'{DAG_DIR}/bronze/bronze_posts.py',
        conn_id='spark_default',
    )
    
    bronze_comments = SparkSubmitOperator(
        task_id='transform_bronze_comments',
        application=f'{DAG_DIR}/bronze/bronze_comments.py',
        conn_id='spark_default',
    )
    
    bronze_likes = SparkSubmitOperator(
        task_id='transform_bronze_likes',
        application=f'{DAG_DIR}/bronze/bronze_likes.py',
        conn_id='spark_default',
    )
    
    silver_keyword = SparkSubmitOperator(
        task_id='transform_silver_keyword',
        application=f'{DAG_DIR}/silver/silver_keyword.py',
        conn_id='spark_default',
    )


    [bronze_users, bronze_posts, bronze_comments, bronze_likes] >> silver_keyword