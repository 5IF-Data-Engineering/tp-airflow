from faker import Faker
import json
import pandas as pd

import random
from urllib.request import urlopen

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
    df.to_csv("character.csv", index=False)
    return df

df = _get_data()
for i in range(len(df)):
    print(df.iloc[i]['level'])