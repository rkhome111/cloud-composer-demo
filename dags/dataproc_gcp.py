from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

default_args = {
  "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
  "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
  "quota_project_id": "spheric-subject-274816",
  "refresh_token": "1//03mKgLRqrFRJqCgYIARAAGAMSNgF-L9Ira3YuH1boWVewCP0aDfjIl5fzVWQ97cTcJAiHuW-ehSlRiyZpsIrq4UViMiBLRv1GHA",
  "type": "authorized_user"
}

CLUSTER_NAME="cluster-dp"
REGION="us-central1"
PROJECT_ID="spheric-subject-274816"
PYSPARK_URI="gs://rkboss-spark-test-data/spark-gcs-hive-example.py"
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"{PYSPARK_URI}",
                    "args":["hello","world"]}
}


with DAG(dag_id='dataproc_submit_example_rk', start_date=datetime(2022, 1, 1),
        schedule_interval='@daily', catchup=False) as dag:

    pyspark_task = DataprocSubmitJobOperator(
        gcp_conn_id="google_cloud_default",
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    pyspark_task