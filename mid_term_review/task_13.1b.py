#!/usr/bin/env python
import pandas as pd
from datetime import datetime
    
#Import data
characters_df = pd.read_csv("./star_wars/characters.csv")
planets_df = pd.read_csv("./star_wars/planets.csv")
species_df = pd.read_csv("./star_wars/species.csv")
starships_df = pd.read_csv("./star_wars/starships.csv")
vehicles_df = pd.read_csv("./star_wars/vehicles.csv")

#merge species and characters
char_species_df = pd.merge(characters_df,species_df,how="left")

#Insight 1: Species of star wars characters. sorted by most common to least
print(char_species_df["species"].dropna().value_counts(sort=True))

#Insight 2: Most populous planets. sorted by most common to least
print(planets_df[['name','population']].dropna().sort_values(by=['population'],ascending=False))

#Insight 3: Largest starships. sorted by longest down.
starships_df['length'] = starships_df['length'].apply(lambda x: str(x).replace(',','.')).astype(float)
print(starships_df[['name','length']].dropna().sort_values(by=['length'],ascending=False))

