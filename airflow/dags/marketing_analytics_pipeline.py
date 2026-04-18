import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.path.expanduser('~/marketing_analytics_pet_project_ae')
DBT_DIR = f'{PROJECT_DIR}/marketing_analytics'
PYTHON = f'{PROJECT_DIR}/venv/bin/python'
VENV_ACTIVATE = f'{PROJECT_DIR}/venv/bin/activate'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='marketing_analytics_pipeline',
    default_args=default_args,
    description='Marketing Analytics dbt pipeline',
    schedule='0 8 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['marketing', 'dbt'],
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command=f'source {VENV_ACTIVATE} && cd {PROJECT_DIR} && {PYTHON} scripts/generate_data.py',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'source {VENV_ACTIVATE} && cd {DBT_DIR} && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'source {VENV_ACTIVATE} && cd {DBT_DIR} && dbt test',
    )

    notify_success = BashOperator(
        task_id='notify_success',
        bash_command=f'source {VENV_ACTIVATE} && cd {PROJECT_DIR} && {PYTHON} scripts/notify_slack.py success',
    )

    generate_data >> dbt_run >> dbt_test >> notify_success