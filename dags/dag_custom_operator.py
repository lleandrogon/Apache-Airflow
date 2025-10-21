from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from custom_operators.Hello_Operator import HelloOperator
import pendulum

with DAG(
    dag_id="DAG_CustomOperator",
    schedule="22 22 * * *",
    start_date=pendulum.datetime(2025, 10, 20, tz="America/Sao_Paulo"),
    catchup=False
) as dag:
    start = EmptyOperator(task_id="start")
    meu_operador = HelloOperator(task_id="hello", name="André Ricardo")
    end = EmptyOperator(task_id="end")

start >> meu_operador >> end