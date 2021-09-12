SET the following:


Set sql_aclhemy_conn = postgresql+psycopg2://<user>:<password>@<host>/<db>
(In if you use Postgres)

executor = LocalExecutor

Execute the following command:

Airflow db init

I have added the user to pg_hba.conf as well



Airflow webserver
Airflow scheduler 
brew services start postgresql


See the documentation:

https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html