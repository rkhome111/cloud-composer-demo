from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
sshHook = SSHHook(ssh_conn_id="gcp-connection", key_file='/opt/airflow/dags/keys/test')

create_command = "./script/test-script.sh"
#sshHook = SSHHook(ssh_conn_id="gcp-connection")

with DAG(dag_id='bash_processing_rk', start_date=datetime(2022, 1, 1), 
        schedule_interval='@daily', catchup=False) as dag:
    
    t2 = BashOperator(
    task_id='run-script',
    bash_command=create_command,
    dag=dag)

    t3 = BashOperator(
    task_id='run-another-script',
    bash_command="./script/another-test.sh",
    dag=dag)

    t5 = SSHOperator(
    ssh_hook=sshHook,
    task_id='test_ssh_operator',
    command='./first-script.sh hello world',
    dag=dag)

    trigger = TriggerDagRunOperator(
        task_id="trigger_id",
        trigger_dag_id="dataproc_submit_example_rk",
        dag=dag,
    )

    #t2 >> t3 >> t5
    t2 >> t3 >> trigger