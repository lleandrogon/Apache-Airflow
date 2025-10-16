from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="a_primeira_DAG_v0",
    start_date=datetime(2025, 10, 16),
    schedule="@daily",
    doc_md=__doc__
):
    start = EmptyOperator(task_id="start")
    hello = BashOperator(task_id="hello", bash_command="echo hello world")
    end = EmptyOperator(task_id="end")

(start >> hello >> end)