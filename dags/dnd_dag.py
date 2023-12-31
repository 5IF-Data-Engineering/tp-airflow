from faker import Faker
import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from urllib.request import urlopen
import json
import pandas as pd

import random

default_args = {
    'start_date': datetime.datetime(2020, 10, 6, 20, 0, 0, 0),
    'owner': 'airflow',
    'interval': '@weekly',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args,
    catchup=False,
)

def _get_data():
    # name of character
    fake = Faker()
    list_names = []
    list_saved_scores = []
    list_race = []
    list_language = []
    list_chosen_class = []
    list_proficiency_choices = []
    list_level = []
    list_spell = []
    # Generate 5 characters
    for i in range(5):
        name = fake.name()
        name = name.replace(" ", "+")
        list_names.append(name)
        # Attributes
        abilities = ["str", "dex", "con", "int", "wis", "cha"]
        list_scores = []
        for ability in abilities:
            with urlopen(f"https://www.dnd5eapi.co/api/ability-scores/{ability}") as response:
                source = response.read()
            data = json.loads(source)
            score = len(data["skills"])
            list_scores.append(str(score))
        saved_scores = '_'.join(list_scores)
        list_saved_scores.append(saved_scores)

        # Race
        with urlopen("https://www.dnd5eapi.co/api/races") as response:
            source = response.read()
        data_race = json.loads(source)
        list_races = data_race["results"]
        race = list_races[random.randint(0, len(list_races)-1)]["name"]
        list_race.append(race)
    
        # languages
        with urlopen("https://www.dnd5eapi.co/api/languages") as response:
            source = response.read()
        data_language = json.loads(source)
        list_languages = data_language["results"]
        language = list_languages[random.randint(0, len(list_languages)-1)]["name"]   
        list_language.append(language)

        # class
        with urlopen("https://www.dnd5eapi.co/api/classes") as response:
            source = response.read()
        data_class = json.loads(source)
        list_classes = data_class["results"]
        random_val = random.randint(0, len(list_classes)-1)
        index_class = list_classes[random_val]["index"]
        chosen_class = list_classes[random_val]["name"]  
        list_chosen_class.append(chosen_class)

        # proficiency choices
        with urlopen(f"https://www.dnd5eapi.co/api/classes/{index_class}") as response:
            source = response.read()
        data_proficiency = json.loads(source)
        nb_choices = data_proficiency["proficiency_choices"][0]["choose"]
        list_options = data_proficiency["proficiency_choices"][0]["from"]["options"]
        proficiency_choices = random.sample(list_options, nb_choices)
        proficiency_choices = [proficiency_choices[i]["item"]["name"] for i in range(len(proficiency_choices))]
        proficiency_choices = [proficiency_choices[i].replace("Skill: ", "") for i in range(len(proficiency_choices))]
        str_proficiency_choices = '_'.join(proficiency_choices)
        list_proficiency_choices.append(str_proficiency_choices)

        # level
        level = random.randint(1, 3)
        list_level.append(level)

        # spells
        with urlopen(f"https://www.dnd5eapi.co/api/classes/{index_class}/levels/{level}/spells") as response:
            source = response.read()
        data_spells = json.loads(source)
        list_spells = [data_spells["results"][i]["name"] for i in range(data_spells["count"])]
        list_spells = [list_spells[i].replace(" ", "+") for i in range(len(list_spells))]
        str_spells = '_'.join(list_spells)
        list_spell.append(str_spells)

    key = ["name", "attributes", "race", "languages", "class", "proficiency_choices", "level", "spells"]
    df = pd.DataFrame(columns=key)
    df["name"] = list_names
    df["attributes"] = list_saved_scores
    df["race"] = list_race
    df["languages"] = list_language
    df["class"] = list_chosen_class
    df["proficiency_choices"] = list_proficiency_choices
    df["level"] = list_level
    df["spells"] = list_spell
    df.to_csv("/opt/airflow/dags/data/character.csv", index=False)
    return df

# first task
get_data = PythonOperator(
    task_id='get_data',
    python_callable=_get_data,
    dag=dnd_dag,
)

# second task
df = _get_data()
query = f"""
    INSERT INTO "character" VALUES
    ({df.iloc[0]["name"]}, {df.iloc[0]["attributes"]}, {df.iloc[0]["race"]}, {df.iloc[0]["languages"]}, {df.iloc[0]["class"]}, {df.iloc[0]["proficiency_choices"]}, {df.iloc[0]["level"]}, {df.iloc[0]["spells"]}),
    ({df.iloc[1]["name"]}, {df.iloc[1]["attributes"]}, {df.iloc[1]["race"]}, {df.iloc[1]["languages"]}, {df.iloc[1]["class"]}, {df.iloc[1]["proficiency_choices"]}, {df.iloc[1]["level"]}, {df.iloc[1]["spells"]}),
    ({df.iloc[2]["name"]}, {df.iloc[2]["attributes"]}, {df.iloc[2]["race"]}, {df.iloc[2]["languages"]}, {df.iloc[2]["class"]}, {df.iloc[2]["proficiency_choices"]}, {df.iloc[2]["level"]}, {df.iloc[2]["spells"]}),
    ({df.iloc[3]["name"]}, {df.iloc[3]["attributes"]}, {df.iloc[3]["race"]}, {df.iloc[3]["languages"]}, {df.iloc[3]["class"]}, {df.iloc[3]["proficiency_choices"]}, {df.iloc[3]["level"]}, {df.iloc[3]["spells"]}),
    ({df.iloc[4]["name"]}, {df.iloc[4]["attributes"]}, {df.iloc[4]["race"]}, {df.iloc[4]["languages"]}, {df.iloc[4]["class"]}, {df.iloc[4]["proficiency_choices"]}, {df.iloc[4]["level"]}, {df.iloc[4]["spells"]});
"""
insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_default',
    sql=query,
    dag=dnd_dag,
    trigger_rule='none_failed',
    autocommit=True,
)   

get_data >> insert_data