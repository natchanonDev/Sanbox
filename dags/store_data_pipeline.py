from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils import timezone
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.hive_operator import HiveOperator

import pandas as pd

DAGS_FOLDER = '/usr/local/airflow/dags'

def get_data():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', schema='breakfast')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        SELECT * FROM store
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    for each in rows:
        print(each)

def dump_data(table):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', schema='breakfast')
    pg_hook.bulk_dump(table, f'{DAGS_FOLDER}/{table}_export')

def _transform_to_csv():
    df = pd.read_csv(f'{DAGS_FOLDER}/store_export', sep='\t', header=None)
    df.to_csv(f'{DAGS_FOLDER}/store_export_cleaned.csv', index=False, header=0)

with DAG('store_data_pipeline', 
         schedule_interval='0 * * * *',
         start_date=timezone.datetime(2021, 3, 25),
         catchup=False) as dag:
    start = DummyOperator(task_id='start')

    query_store_data = PostgresOperator(
        task_id='query_store_data',
        postgres_conn_id='my_postgres_conn',
        sql='''
            SELECT * FROM store LIMIT 10
        '''
    )

    t2 = PythonOperator(
        task_id='hook_task',
        python_callable=get_data,
    )

    t3 = PythonOperator(
        task_id='dump_store_task',
        python_callable=dump_data,
        op_kwargs={'table': 'store'},
    )

    is_file_available = FileSensor(
        task_id='is_file_available',
        fs_conn_id='my_file_conn',
        filepath='store_export',
        poke_interval=5,
        timeout=100,
    )

    upload_raw_data_to_hdfs = BashOperator(
        task_id='upload_raw_data_to_hdfs',
        bash_command=f'hdfs dfs -put -f {DAGS_FOLDER}/store_export /landing'
    )

    transform_to_csv = PythonOperator(
        task_id='transform_to_csv',
        python_callable=_transform_to_csv
    )

    upload_clean_data_to_hdfs = BashOperator(
        task_id='upload_clean_data_to_hdfs',
        bash_command=f'hdfs dfs -put -f {DAGS_FOLDER}/store_export_cleaned.csv /cleaned'
    )

    # Create Hive table
    create_store_lookup_table = HiveOperator(
        task_id='create_store_lookup_table',
        hive_cli_conn_id='my_hive_conn',
        hql='''
            CREATE TABLE IF NOT EXISTS dim_store_lookup (
                store_id                 INT,
                store_name               VARCHAR(100),
                address_city_name        VARCHAR(300),
                address_state_prov_code  VARCHAR(2),
                msa_code                 VARCHAR(100),
                seg_value_name           VARCHAR(100),
                parking_space_qty        DECIMAL(38, 2),
                sales_area_size_num      INT,
                avg_weekly_baskets       DECIMAL(38, 2)
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
            STORED AS TEXTFILE;
        '''
    )

    # Load data in Hive table
    load_data_to_hive_table = HiveOperator(
        task_id='load_data_to_hive_table',
        hive_cli_conn_id='my_hive_conn',
        hql=f'''
            LOAD DATA INPATH '/cleaned/store_export_cleaned.csv' OVERWRITE INTO TABLE dim_store_lookup;
        '''
    )

    end = DummyOperator(task_id='end')

    start >> query_store_data
    start >> t2 >> t3 >> is_file_available >> transform_to_csv >> upload_clean_data_to_hdfs >>\
        create_store_lookup_table >> load_data_to_hive_table >> end
    is_file_available >> upload_raw_data_to_hdfs