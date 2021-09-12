from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date':datetime(2021,9,5)
}


with DAG(
    'Pararell_Dag',
    default_args=default_args,
    schedule_interval='@daily',
    description='Local executor test DAG',
    catchup=False
) as dag:



    first_task = BashOperator(
        task_id='first_task',
        bash_command='sleep 0.5'

    )

    second_p_task = BashOperator(
        task_id='second_task',
        bash_command='sleep 0.5'
  
    )

    third_p_task = BashOperator(
        task_id='third_task',
        bash_command='sleep 0.5'
    )

    fourth_task = BashOperator(
        task_id='fourth_task',
        bash_command='sleep 0.5'
    )

first_task >> [second_p_task,  third_p_task] >> fourth_task
