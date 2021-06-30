# 1. import packages
# 2. define DAG
# 3. use Operator
# 4. create task
# 5. define dependency

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy_operator import DummyOperator

with DAG('my_dag', 
         schedule_interval='0 * * * *',
         start_date=timezone.datetime(2021, 3, 25),
         catchup=False) as dag:
    start = DummyOperator(
        task_id='start'
    )
    end = DummyOperator(
        task_id='end'
    )

    start >> end