from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import subprocess

default_args = {
    'owner': 'assignment3',
    'start_date': datetime(2025,1,1),
    'retries': 0
}

def run_train():
    subprocess.run(["python", "/opt/airflow/dags/processing/ml/train.py"], check=True)

with DAG('ml_train', default_args=default_args, schedule_interval=timedelta(seconds=20), catchup=False) as dag:
    t = PythonOperator(task_id="train_model", python_callable=run_train)
