import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# Find the closest country to a given latitude and longitude
import pandas as pd
import json
import math

# Constants
R = 6371 # Earth's radius in km

def _closest_country():
    # Read the data
    countries = pd.read_csv('/opt/airflow/dags/data/countries.csv', index_col=['country'])
    lat_lon = json.load(open('/opt/airflow/dags/data/iss-now.json'))['iss_position']
    lat = float(lat_lon['latitude'])
    lon = float(lat_lon['longitude'])
    radian_lat = math.radians(lat)
    radian_lon = math.radians(lon)

    # For each country, compute the distance to the given coordinates
    list_distance = []
    for index, row in countries.iterrows():
        country_lat = math.radians(row['latitude'])
        country_lon = math.radians(row['longitude'])
        ## Calculate the angle between the two points
        # Haversine formula
        dlon = country_lon - radian_lon 
        dlat = country_lat - radian_lat
        a = math.sin(dlat / 2)**2 + math.cos(radian_lat) * math.cos(country_lat) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        dist = R * c
        list_distance.append({'country': index, 'distance': dist})

    # Find the closest country
    df_distance = pd.DataFrame(list_distance)
    df_distance = df_distance.sort_values(by=['distance'])
    closest_country = df_distance.iloc[0]['country']

    # Save closest country to file
    with open('/opt/airflow/dags/data/closest_country.txt', 'w') as f:
        f.write(f"{closest_country},{countries.loc[closest_country]['latitude']},{countries.loc[closest_country]['longitude']},{countries.loc[closest_country]['name']},{countries.loc[closest_country]['continent']}")

def _read_closest_country():
    with open('/opt/airflow/dags/data/closest_country.txt', 'r') as f:
        country = f.read()
        returned_country = country.split(',')
        returned_dic = {'country': returned_country[0], 'latitude': returned_country[1], 
                        'longitude': returned_country[2], 'name': returned_country[3], 
                        'continent': returned_country[4]}
    return returned_dic

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

iss_dag = DAG(
    dag_id='iss_dag',
    default_args=default_args,
    catchup=False,
)

# first task
first_task = BashOperator(
    task_id='first_task',
    bash_command='curl http://api.open-notify.org/iss-now.json -H "accept: application/json" > /opt/airflow/dags/data/iss-now.json',
    dag=iss_dag,
)

# second task
second_task = PythonOperator(
    task_id='second_task',    
    trigger_rule='all_success',
    python_callable=_closest_country,
    dag=iss_dag,
)

# third task
closest_country = _read_closest_country()
third_task = PostgresOperator(
    task_id='third_task',
    postgres_conn_id='postgres_default',
    sql=f"INSERT INTO country (country, latitude, longitude, name, continent) VALUES ('{closest_country['country']}', {closest_country['latitude']}, {closest_country['longitude']}, '{closest_country['name']}', '{closest_country['continent']}');",
    dag=iss_dag,
    trigger_rule='none_failed',
    autocommit=True,
)

# fourth task
fourth_task = PostgresOperator(
    task_id='fourth_task',
    postgres_conn_id='postgres_default',
    sql="SELECT continent FROM country;",
    dag=iss_dag,
)

first_task >> second_task >> third_task >> fourth_task