export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
airflow db init

airflow api-server -p 8080
airflow dag-processor
airflow scheduler
