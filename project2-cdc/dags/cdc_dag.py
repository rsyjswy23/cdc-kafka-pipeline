from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'cdc_producer_scheduler',
    default_args=default_args,
    description='Run CDC Producer every 2 minutes',
    schedule_interval='*/2 * * * *',
    catchup=False
)

run_producer = BashOperator(
    task_id='run_cdc_producer',
    bash_command='cd /opt/airflow/producers && python cdc_producer.py --once',
    dag=dag
)

run_producer