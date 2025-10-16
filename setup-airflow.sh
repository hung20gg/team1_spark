export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
# export AIRFLOW__API__PORT=8888
airflow db migrate

airflow api-server -p 8080 -D
airflow dag-processor -D
airflow scheduler -D
