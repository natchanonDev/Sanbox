import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import timezone


def _sleep():
    time.sleep(3)


default_args = {
    'owner': 'natchanon',
    'start_date': timezone.datetime(2021, 3, 1),
    'email': ['airflow@example.com'],
    'sla': timedelta(seconds=10),
}
with DAG('test_sla',
         default_args=default_args,
         description='Test SLA',
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:

    first_check = PythonOperator(
        task_id='first_check',
        python_callable=_sleep,
        sla=timedelta(seconds=2),
    )

    second_check = PythonOperator(
        task_id='second_check',
        python_callable=_sleep,
    )

    first_check >> second_check