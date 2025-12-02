"""
incremental_dag.py
Airflow DAG that runs the incremental Spark script every 4 seconds.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import subprocess

default_args = {
    'owner': 'assignment3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
}

def run_incremental():
    subprocess.run(["python", "/opt/airflow/dags/processing/incremental/spark_aggregate_incremental.py"], check=True)

with DAG('incremental_aggregation', default_args=default_args, schedule=timedelta(seconds=4), catchup=False) as dag:
    task = PythonOperator(
        task_id="run_incremental_job",
        python_callable=run_incremental
    )
