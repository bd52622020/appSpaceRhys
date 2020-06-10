#!/usr/bin/env python
import pandas as pd
from datetime import datetime
    
#Import data
data_df_columns = ["user id", "item id", "rating", "timestamp"]
data_df = pd.read_csv("./ml-100k/u.data", delim_whitespace=True, names=data_df_columns)
item_df_columns = ["movie id", "movie title", "release date", "video release date",\
"IMDb URL", "unknown", "Action", "Adventure", "Animation",\
"Children's", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",\
"Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",\
"Thriller", "War", "Western"]
item_df = pd.read_csv("./ml-100k/u.item", encoding = "ISO-8859-1", sep='|', names=item_df_columns)
user_df_columns = ["user id" , "age" , "gender" , "occupation" , "zip code"]
user_df = pd.read_csv("./ml-100k/u.user", encoding = "ISO-8859-1", sep='|', names=user_df_columns)
data_df.rename(columns={"item id": "movie id"},inplace=True)
full_df = pd.merge(data_df,item_df,how="outer")
full_df = pd.merge(full_df,user_df,how="outer")
print()

#Print names of the films with most reviews, sorted alphabetically.
print("Movies with the most reviews. Unsorted;")
top_ten = full_df["movie title"].value_counts(sort=True)[:10]
print(*top_ten.sort_index(axis=0).index.values,sep='\n')

#Print names of the films with most reviews, sorted by number of reviews.
print("\nMovies with the most reviews. Sorted by number of reviews, descending;")
print(*top_ten.index.values,sep='\n')

#Print list of the number of rating by genre
print("\nGenres with the most reviews;")
print(full_df.loc[:,'unknown':'Western'].sum().reset_index().to_string(header=None, index=None))

#print oldest movie with a 5 rating.
pd.options.mode.chained_assignment = None 
print("\nOldest movie with a 5 rating;")
five_df = full_df.loc[full_df['rating'] == 5]
five_df["release date"] = pd.to_datetime(five_df["release date"])
print(*five_df.loc[five_df["release date"] == five_df["release date"].min()]["movie title"].drop_duplicates().values,sep='\n')

#Print a list of the genre of the top 10 most rated movies. 
top_ten_info_df = item_df[item_df["movie title"].isin(top_ten.index)]
top_ten_info_df["genre list"] = top_ten_info_df.loc[:,'unknown':'Western'].astype(int).dot(top_ten_info_df.loc[:,'unknown':'Western'].columns+',').str[:-1]
pd.options.mode.chained_assignment = 'warn'
top_ten_info_df = top_ten_info_df[["movie title","genre list"]]
print("\nGenres of the top ten most rated movies;")
print(top_ten_info_df.to_string(header=None, index=None))

#Print the title of the movie that was rated the most by students 
print("\nMovie with most ratings;")
print(*full_df.loc[(full_df['occupation'] == 'student')]["movie title"].value_counts(sort=True)[:1].index.values,sep="\n")

#Print the list of movies that received the highest number of “5” rating 
print("\nTen movies with the most 5 ratings;")
print(*five_df["movie title"].value_counts(sort=True)[:10].index.values,sep='\n')

#Print the list of zip codes corresponding to the highest number of users that rated movies. 
print("\nTen zip codes with the most ratings;")
print(user_df["zip code"].value_counts(sort=True)[:10].reset_index().to_string(header=None, index=None))

#Find the most rated movie by users in the age group 20 to 25.
print("\nMost rated movie by users between 20 and 25;")
print(*full_df.loc[(full_df['age'] <= 25) & (full_df['age'] >= 20)]["movie title"].value_counts(sort=True)[:1].index.values,sep="\n")

#Print the list of movies that were rate after year 1960. 
print("\nMovies rated after 1960;")
cutoff = datetime.timestamp(datetime(1961,1,1))
print(full_df.loc[full_df["timestamp"] >= cutoff]["movie title"])

#Convert UnixTimeStamp in  timestamp column of u.data to human readable date 
print("\nConvert Unix timestamp to datetime;")
print(pd.to_datetime(full_df["timestamp"],unit='s'))
