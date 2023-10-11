import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="test_dag",
    start_date=datetime.datetime.now(),
    catchup=False,
    schedule_interval=None,
    max_active_runs=1,
) as dag:

    # Test dag
    EmptyOperator(task_id="test")
