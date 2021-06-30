from airflow import DAG
from airflow.utils import timezone
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def _print_context(**kwargs):
    print(kwargs)
    ds = kwargs['ds']
    print(ds)


with DAG('my_templated_dag', 
         schedule_interval='0 0 * * MON',
         start_date=timezone.datetime(2021, 3, 1),
         catchup=True) as dag:
    start = DummyOperator(task_id='start')

    echo = BashOperator(
        task_id='echo',
        bash_command='echo {{ ds }} {{ yesterday_ds }} {{ tomorrow_ds }}'
    )

    print_context = PythonOperator(
        task_id='print_context',
        python_callable=_print_context,
        provide_context=True,
        # op_kwargs={'ds': '{{ ds }}'}
    )

    end = DummyOperator(task_id='end')

    start >> [echo, print_context] >> end