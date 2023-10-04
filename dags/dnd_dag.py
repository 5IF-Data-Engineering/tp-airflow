from faker import Faker
import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args,
    catchup=False,
)

# Create `character` table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql='sql/create_table.sql',
    dag=dnd_dag,
)

create_table