import os
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryCreateExternalTableOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from python_script import extract_file

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'test_dataset')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'Country')

dag = models.DAG(
    dag_id='gcs_to_bq_operator',
    start_date=days_ago(2),
    schedule_interval='@once',
    tags=['example'],
)

create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
)

# [START howto_operator_gcs_to_bigquery]
load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery_example',
    bucket='rsk-bkt1',
    source_objects=['countries.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {'name': 'Year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'result', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'startdate', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'enddate', 'type': 'DATE', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    dag=dag,
)

create_external_table = BigQueryCreateExternalTableOperator(
    task_id="create_external_table",
    destination_project_dataset_table=f"{DATASET_NAME}.external_table",
    bucket='uprn',
    source_objects=['countries.csv'],
    schema_fields=[
        {'name': 'Year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'result', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'startdate', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'enddate', 'type': 'DATE', 'mode': 'NULLABLE'}
    ],
)

do_stuff1 = PythonOperator(
        task_id="pyton_work",
        python_callable=extract_file.main,  # assume entrypoint is main()
        dag=dag,
    )

get_data = BigQueryGetDataOperator(
    task_id='get_data_from_bq',
    dataset_id='test_dataset',
    table_id='Country',
    max_results=100,
    selected_fields='Year,Country,number,result',
    # bigquery_conn_id='airflow-service-account'
)

read_data_from_bigquery = PythonOperator(
        task_id="read_data",
        python_callable=extract_file.read_bigquery_response,  # assume entrypoint is main()
        dag=dag,
    )

# unzip = UnzipOperator(
#     task_id="unzip_archive",
#     path_to_zip_file=archive_path,
#     path_to_unzip_contents=unzipped_f_path)

do_stuff1 >> create_test_dataset >> load_csv >> get_data >> read_data_from_bigquery >> create_external_table
