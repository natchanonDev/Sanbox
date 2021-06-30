from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.operators.hive_operator import HiveOperator


default_args = {
    'owner' : 'natchanon'
}
with DAG('product_price_range_pipeline', 
         schedule_interval='0 0 * * THU',
         start_date=timezone.datetime(2020, 8, 15),
         default_args=default_args,
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    
    check_named_partition = NamedHivePartitionSensor(
        task_id='check_named_partition',
        partition_names=['fact_transactions/execution_date={{ macros.ds_add(ds, -1) }}'],
        metastore_conn_id='my_hive_metastore_conn',
        poke_interval=3,
    )

    create_product_transactions_table = HiveOperator(
        task_id='create_product_transactions_table',
        hive_cli_conn_id='my_hive_conn',
        hql='''
            CREATE TABLE IF NOT EXISTS natchanon_product_transactions (
                product_description STRING,
                price               DECIMAL(10, 2),
                units               INT,
                visits              INT
            )
            PARTITIONED BY (execution_date DATE)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
            STORED AS TEXTFILE;
        '''
    )

    add_new_product_transactions = HiveOperator(
        task_id='add_new_product_transactions',
        hive_cli_conn_id='my_hive_conn',
        hiveconfs={'hive.exec.dynamic.partition.mode': 'nonstrict'},
        hql='''
            INSERT INTO TABLE natchanon_product_transactions
            SELECT dim_product_lookup.description,
                fact_transactions.price,
                fact_transactions.units,
                fact_transactions.visits,
                fact_transactions.execution_date
            FROM fact_transactions
            JOIN dim_product_lookup ON fact_transactions.upc = dim_product_lookup.upc
            WHERE fact_transactions.execution_date = '{{ macros.ds_add(ds, -1) }}'
        '''
    )

    end = DummyOperator(task_id='end')

    start >> check_named_partition >> create_product_transactions_table >> add_new_product_transactions >> end