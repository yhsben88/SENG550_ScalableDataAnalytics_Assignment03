from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import subprocess

default_args = {'owner': 'assignment3', 'start_date': datetime(2025,1,1)}

def run_push_predictions():
    subprocess.run(["python", "/opt/airflow/dags/processing/ml/push_predictions_to_redis.py"], check=True)

with DAG('ml_push_predictions', default_args=default_args, schedule_interval=timedelta(seconds=20), catchup=False) as dag:
    t = PythonOperator(task_id="push_predictions", python_callable=run_push_predictions)
