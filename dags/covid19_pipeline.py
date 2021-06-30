import json
from datetime import datetime

from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
import pandas as pd

import os
import requests
import logging

DATA_FOLDER = '/opt/airflow/files'
BUCKET_NAME = 'workshop-covid19-bucket'

LANDING = 'LANDING'
CLEANED = 'CLEANED'
TRANSACTION = 'TRANSACTION'

DATASET_NAME = 'WORKSHOP'
TABLE_NAME = 'COVID19_DATA'


def _get_data_from_api():
    url = 'https://covid19.th-stat.com/api/open/today'
    response = requests.get(url)
    data = response.json()
    with open(f'{DATA_FOLDER}/raw_data.json', 'w') as f:
        json.dump(data, f)

    # url = "https://covid-19-data.p.rapidapi.com/report/country/name"
    # querystring = {"name":"Thailand","date":"2021-05-24"}
    # headers = {
    #     'x-rapidapi-key': "bda79c440amsh0d9a37ecd07a0b2p1051edjsn9a659215eb15",
    #     'x-rapidapi-host': "covid-19-data.p.rapidapi.com"
    #     }
    # response = requests.request("GET", url, headers=headers, params=querystring)
    # data = response.json()
    # print(response.json)
    # with open(f'{DATA_FOLDER}/raw_data.json', 'w') as f:
    #     json.dump(data, f)

    return data

def _transform_to_csv():
    # df = pd.read_json(f'{DATA_FOLDER}/raw_data.json', typ='series')
    # print(df[df])
    with open(f'{DATA_FOLDER}/raw_data.json', 'r') as f:
        data = json.load(f)
        df = pd.DataFrame([data])
        print(df)
    export_csv = df.to_csv(f'{DATA_FOLDER}/export_data.csv', index=False, header=True)

def _query_data_by_date(date):
    df = pd.read_csv(f'{DATA_FOLDER}/export_data.csv')
    print(df)
    print(df.UpdateDate[0])
    date_obj = datetime.strptime(df.UpdateDate[0], "%d/%m/%Y %H:%M")
    df['UpdateDate'] = datetime.strftime(date_obj, "%Y-%m-%d")
    print(df)
    new_df = df[df.UpdateDate == date]
    print(new_df)
    new_df.to_csv(f'{DATA_FOLDER}/data-{date}.csv', index=False)

default_args = {
    'owner': 'de_tangerine',
    'start_date': datetime(2021, 5, 18),
    'schedule_interval':'0 12 * * *',
    'email': ['natchanon.t@tangerine.co.th'],
    'tags': ['workshop'],
    'provide_context': True,
}

with DAG('covid19_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='Data pipeline for COVID-19 report',
         catchup=False) as dag:
    
    start = DummyOperator(task_id='start')

    get_data_from_api = PythonOperator(
        task_id='get_data_from_api',
        python_callable=_get_data_from_api
    )

    transform_to_csv = PythonOperator(
        task_id='transform_to_csv',
        python_callable=_transform_to_csv
    )

    upload_to_landing_zone = LocalFilesystemToGCSOperator(
        task_id='upload_to_landing_zone',
        gcp_conn_id='google_cloud_default',
        src=f'{DATA_FOLDER}/export_data.csv',
        dst=f'{LANDING}/export_data.csv',
        bucket=f'{BUCKET_NAME}'
    )

    query_data_by_date = PythonOperator(
        task_id='query_data_by_date',
        python_callable=_query_data_by_date,
        op_kwargs={'date' : '{{ ds }}'},
    )

    upload_to_cleaned_zone = LocalFilesystemToGCSOperator(
        task_id='upload_to_cleaned_zone',
        gcp_conn_id='google_cloud_default',
        src=f'{DATA_FOLDER}/data-{{{{ ds }}}}.csv',
        dst=f'{CLEANED}/data-{{{{ ds }}}}.csv',
        bucket=f'{BUCKET_NAME}'
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=f'{BUCKET_NAME}',
        source_objects=[f'{CLEANED}/data-{{{{ ds }}}}.csv'],
        destination_project_dataset_table=f'{DATASET_NAME}.{TABLE_NAME}',
        autodetect=True,
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1
    )

    end = DummyOperator(task_id='end')


    start >> get_data_from_api >> transform_to_csv >> upload_to_landing_zone >> \
     query_data_by_date >> upload_to_cleaned_zone >> gcs_to_bq >> end