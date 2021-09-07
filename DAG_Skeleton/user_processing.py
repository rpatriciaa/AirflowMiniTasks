from datetime import datetime
from textwrap import dedent
import json
from pandas import json_normalize
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date':datetime(2021,9,5)
}

def f_process_user(ti):
    users = ti.xcom_pull(task_ids=['extract_user'])
    user = users[0]['results'][0]
    processed = json_normalize({
       'firstname': user['name']['first'],
       'lastname': user['name']['last'],
       'county': user['location']['country'],
       'username': user['login']['username'],
       'password': user['login']['password'],
    })
    processed.to_csv('/Users/racsikpatricia/airflow/dags/processed.csv', index=None, header=False)

with DAG(
    'user_processing',
    default_args=default_args,
    schedule_interval='@daily',
    description='Udemy DAG',
    catchup=False
) as dag:


    creating_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id="dev_database_default",
        sql='''
            DROP TABLE IF EXISTS dev_database.users;
            CREATE TABLE dev_database.users(
                user_id SERIAL PRIMARY KEY,
                firstname VARCHAR(40) NOT NULL,
                lastname VARCHAR(40) NOT NULL,
                country VARCHAR(20) NOT NULL,
                username VARCHAR(40) NOT NULL,
                password VARCHAR(10) NOT NULL
                );
            ''',
    )

    is_api_ok = HttpSensor(
        task_id='api_ok',
        http_conn_id='user_api',
        endpoint='api/'
    )

    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=f_process_user
    )

    storing_user = BashOperator(
        task_id='storing_user',
        bash_command='file.sh'
    )

creating_table >> is_api_ok >> extract_user >> process_user >> storing_user