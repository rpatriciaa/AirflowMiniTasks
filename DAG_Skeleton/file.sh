psql -d devdb -c "COPY dev_database.users(firstname, lastname, country, username, password) from '/Users/racsikpatricia/airflow/dags/processed.csv' delimiter ',' csv "
