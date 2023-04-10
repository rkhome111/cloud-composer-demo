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
from python_script import read_config

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'poc_dataset')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'Country')

dag = models.DAG(
    dag_id='gcp_bq_poc',
    start_date=days_ago(2),
    schedule_interval='@daily',
    tags=['example'],
)

create_poc_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
)

read_configuration = PythonOperator(
        task_id="pyton_work",
        python_callable=read_config.main,  # assume entrypoint is main()
        dag=dag,
    )

create_poc_dataset >> read_configuration