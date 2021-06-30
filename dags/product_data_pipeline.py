from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils import timezone

import great_expectations as ge
import pandas as pd

DAGS_FOLDER = '/usr/local/airflow/dags'

def get_data():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', schema='breakfast')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        SELECT * FROM product
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    for each in rows:
        print(each)

def dump_data(table):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn', schema='breakfast')
    pg_hook.bulk_dump(table, f'{DAGS_FOLDER}/{table}_export')

def convert_to_lt(value):
    number, unit = value.split(' ')
    if unit == 'OZ':
        return float(number) * 0.0295735
    if unit == 'LT':
        return float(number)
    if unit == 'ML':
        return float(number) / 1000
    if unit == 'CT':
        return float(number) * 0.00001

def _transform_to_csv():
    df = pd.read_csv(f'{DAGS_FOLDER}/product_export', sep='\t', header=None)
    # print(df.head())
    # print(df[5])
    df[5] = df[5].map(convert_to_lt)
    df.to_csv(f'{DAGS_FOLDER}/product_export_cleaned.csv', index=False, header=0)

def _validate_range():
    my_df = ge.read_csv(f'{DAGS_FOLDER}/product_export_cleaned.csv', header=None)
    print(my_df)
    results = my_df.expect_column_values_to_be_between(
        column=5,
        min_value=0,
        max_value=2,
    )
    assert results['success'] is True

with DAG('product_data_pipeline', 
         schedule_interval='0 * * * *',
         start_date=timezone.datetime(2021, 3, 25),
         catchup=False) as dag:
    start = DummyOperator(task_id='start')

    query_product_data = PostgresOperator(
        task_id='query_product_data',
        postgres_conn_id='my_postgres_conn',
        sql='''
            SELECT * FROM product LIMIT 10
        '''
    )

    t2 = PythonOperator(
        task_id='hook_task',
        python_callable=get_data,
    )

    t3 = PythonOperator(
        task_id='dump_product_task',
        python_callable=dump_data,
        op_kwargs={'table': 'product'},
    )

    from airflow.contrib.sensors.file_sensor import FileSensor

    is_file_available = FileSensor(
        task_id='is_file_available',
        fs_conn_id='my_file_conn',
        filepath='product_export',
        poke_interval=5,
        timeout=100,
    )

    upload_raw_data_to_hdfs = BashOperator(
        task_id='upload_raw_data_to_hdfs',
        bash_command=f'hdfs dfs -put -f {DAGS_FOLDER}/product_export /landing'
    )

    transform_to_csv = PythonOperator(
        task_id='transform_to_csv',
        python_callable=_transform_to_csv
    )

    validate_range = PythonOperator(
        task_id='validate_range',
        python_callable=_validate_range
    )

    upload_clean_data_to_hdfs = BashOperator(
        task_id='upload_clean_data_to_hdfs',
        bash_command=f'hdfs dfs -put -f {DAGS_FOLDER}/product_export_cleaned.csv /cleaned'
    )

    from airflow.operators.hive_operator import HiveOperator

    create_product_lookup_table = HiveOperator(
        task_id='create_product_lookup_table',
        hive_cli_conn_id='my_hive_conn',
        hql='''
            CREATE TABLE IF NOT EXISTS dim_product_lookup (
                upc           VARCHAR(100),
                description   VARCHAR(300),
                manufacturer  VARCHAR(100),
                category      VARCHAR(100),
                sub_category  VARCHAR(100),
                product_size  DECIMAL(38, 2)
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
        ''',
    )

    prepare_to_load_data = BashOperator(
        task_id='prepare_to_load_data',
        bash_command='hdfs dfs -cp -f /cleaned/product_export_cleaned.csv /cleaned/product_export_cleaned_ready.csv'
    )

    load_data_to_hive_table = HiveOperator(
        task_id='load_data_to_hive_table',
        hive_cli_conn_id='my_hive_conn',
        hql=f'''
            LOAD DATA INPATH '/cleaned/product_export_cleaned_ready.csv' OVERWRITE INTO TABLE dim_product_lookup;
        ''',
    )

    end = DummyOperator(task_id='end')

    start >> query_product_data
    start >> t2 >> t3 >> is_file_available >> transform_to_csv >> validate_range >> \
        upload_clean_data_to_hdfs >> create_product_lookup_table >> \
        prepare_to_load_data >> load_data_to_hive_table >> end
    is_file_available >> upload_raw_data_to_hdfs
