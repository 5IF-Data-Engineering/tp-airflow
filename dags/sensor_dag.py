import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sql import BranchSQLOperator

from urllib.request import urlopen
import json
import pandas as pd


def _get_five_users_and_save_to_csv():
    with urlopen("https://randomuser.me/api/?results=5&inc=name,gender,dob,nat&noinfo") as response:
        source = response.read()
    data = json.loads(source)
    list_users = []
    for d in data['results']:
        dic_user = {'gender': d['gender'], 'title': d['name']['title'],
                    'first_name': d['name']['first'], 'last_name': d['name']['last'],
                    'country': d['nat'], 'age': d['dob']['age']}
        list_users.append(dic_user)
    df = pd.DataFrame(list_users)
    df.to_csv('/opt/airflow/dags/data/users.csv', index=False)
    return list_users


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

sensor_dag = DAG(
    dag_id='sensor_dag',
    default_args=default_args,
    catchup=False,
)

# first task
first_task = PythonOperator(
    task_id='first_task',
    python_callable=_get_five_users_and_save_to_csv,
    trigger_rule='all_success',
    dag=sensor_dag,
)

# second task
user_res = _get_five_users_and_save_to_csv()
query = """
    CREATE OR REPLACE FUNCTION age_is_increased() RETURNS BOOLEAN AS $$
    DECLARE
        average_age_before FLOAT;
        average_age_after FLOAT;
    BEGIN
        SELECT AVG(age) INTO average_age_before FROM users;

        INSERT INTO users (gender, title, first_name, last_name, country, age) 
        VALUES
        (%s, %s, %s, %s, %s, %s);
        INSERT INTO users (gender, title, first_name, last_name, country, age) 
        VALUES
        (%s, %s, %s, %s, %s, %s);
        INSERT INTO users (gender, title, first_name, last_name, country, age) 
        VALUES
        (%s, %s, %s, %s, %s, %s);
        INSERT INTO users (gender, title, first_name, last_name, country, age) 
        VALUES
        (%s, %s, %s, %s, %s, %s);
        INSERT INTO users (gender, title, first_name, last_name, country, age) 
        VALUES
        (%s, %s, %s, %s, %s, %s);

        SELECT AVG(age) INTO average_age_after FROM users;

        IF average_age_after > average_age_before THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;
    END;
    $$ LANGUAGE plpgsql;
    SELECT age_is_increased();
    """
params = (user_res[0]["gender"], user_res[0]["title"], user_res[0]["first_name"], user_res[0]["last_name"], user_res[0]["country"], user_res[0]["age"],
          user_res[1]["gender"], user_res[1]["title"], user_res[1]["first_name"], user_res[1]["last_name"], user_res[1]["country"], user_res[1]["age"],
          user_res[2]["gender"], user_res[2]["title"], user_res[2]["first_name"], user_res[2]["last_name"], user_res[2]["country"], user_res[2]["age"],
          user_res[3]["gender"], user_res[3]["title"], user_res[3]["first_name"], user_res[3]["last_name"], user_res[3]["country"], user_res[3]["age"],
          user_res[4]["gender"], user_res[4]["title"], user_res[4]["first_name"], user_res[4]["last_name"], user_res[4]["country"], user_res[4]["age"])
second_task = BranchSQLOperator(
    task_id='second_task',
    conn_id='postgres_default',
    sql=query,
    dag=sensor_dag,
    follow_task_ids_if_true='third_task',
    follow_task_ids_if_false='fourth_task',
    database='airflow',
    parameters=params,
)

# third task
third_task = BashOperator(
    task_id='third_task',
    bash_command='echo "Age increased"',
    dag=sensor_dag,
)

# fourth task
fourth_task = BashOperator(
    task_id='fourth_task',
    bash_command='echo "Age not increased"',
    dag=sensor_dag,
)

# end task
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "End of the DAG"',
    dag=sensor_dag,
    trigger_rule='none_failed',
)

first_task >> second_task >> [third_task, fourth_task]
[third_task, fourth_task] >> end_task
