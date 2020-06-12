#!/usr/bin/env python


from pyspark import SparkContext
from pyspark import SparkConf
from datetime import datetime
from pyspark.sql import SQLContext
from itertools import chain
import pyspark.sql.functions as F


#This function returns a movie names dict.
def loadMovieNames():
    movie_names = {}
    with open("./ml-100k/u.item",encoding = "ISO-8859-1") as f:
        for line in f: # for each line
            fields = line.split('|') #turn line into list of items
            movie_names[int(fields[0])] = fields[1] #adds name to dict with movie id as the name's key.
    return movie_names

#This function returns a genres dict.
def loadGenres():
    genre_names = {}
    with open("./ml-100k/u.genre",encoding = "ISO-8859-1") as f:
        for line in f: # for each line
            fields = line.split('|') #turn line into list of items
            if len(fields) == 2:
                genre_names[int(fields[1])] = fields[0] #adds genre to dict with genre id as the key.
    return genre_names

#function to parse files according to provided schema
def parseFile(line, types, selection, sep=None):
    fields = line.split(sep)
    result = []
    for idx, item in enumerate(fields):
        if idx in selection:
            if types[idx] == 'int': 
                try:   
                    result.append(int(item))
                except:
                    result.append(item)
            elif types[idx] == 'float':
                try:
                    result.append(float(item))
                except:
                    result.append(item)
            elif types[idx] == 'timestamp': 
                try:   
                    result.append(datetime.fromtimestamp(int(item)))
                except:
                    result.append(datetime.min) #keeps dtype consistency and is easy to filter. Null also an option but might cause exceptions.              
            elif types[idx] == 'date':
                try:
                    result.append(datetime.strptime(item, '%d-%b-%Y')) #Assumes certain format. Could use inference but inefficient.
                except:
                    result.append(datetime.min) #keeps dtype consistency and is easy to filter. Null also an option but might cause exceptions.         
            else:
                result.append(item)  
    return result


def genreReplace(line_dict,name_dict):
    result = []
    result.append(name_dict[line_dict['movie_id']])
    result.append(': ')
    line_dict.pop('movie_id')
    line_dict.pop('count')
    for k,v in line_dict.items():
        if v == 1:
            result.append(k)
            result.append(', ')
    result = ''.join(result)
    return result   
    
def main():
    
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)
    sq = SQLContext(sc)

    # Load necessary files
    data_f = sc.textFile("./ml-100k/u.data")
    user_f = sc.textFile("./ml-100k/u.user")
    item_f = sc.textFile("./ml-100k/u.item")
    
    # Define types
    data_t = ['int','int','float','timestamp']
    user_t = ['int', 'int', 'str', 'str', 'str']
    item_t = ['int','str', 'date', 'date', 'str', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int'
              , 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int']
    
    # Create dicts, lists and alias maps
    movie_names = loadMovieNames()
    name_map = F.create_map([F.lit(x) for x in chain(*movie_names.items())])
    genre_names = loadGenres()
    genre_cols = list(genre_names.values())
    genre_cols.insert(0,'movie_id')
    genre_range = list(range(5,24))
    genre_range.insert(0,0)    
        
    # Parse files
    item_date = sq.createDataFrame(item_f.map(lambda f: parseFile(f, item_t, [0,2],'|')),['movie_id','release_date'])
    item_genre = sq.createDataFrame(item_f.map(lambda f: parseFile(f, item_t, genre_range,'|')),genre_cols)
    data_g = sq.createDataFrame(data_f.map(lambda f: parseFile(f, data_t, [0,1,2,3])),['user_id','movie_id','rating','timestamp'])    
    user_g = sq.createDataFrame(user_f.map(lambda f: parseFile(f, user_t, [0,1,3,4],'|')),["user_id", "age", "occupation", "zip_code"])
        
    # Q1.Print names of the films with most reviews, unsorted.  
    print("\nQuestion 1:")
    top_ten_ratings = data_g.select('movie_id').groupBy('movie_id').count().orderBy('count', ascending=False).limit(10)
    top_ten_ratings.select(name_map[data_g['movie_id']].alias('movie_name')).show(truncate=False)
    
    # Q2.Print names of the films with most reviews, sorted alphabetically.
    print("\nQuestion 2:")
    top_ten_ratings.select(name_map[data_g['movie_id']].alias('movie_name')).orderBy('movie_name', ascending=True).show(truncate=False)
    
    # Q3.Print list of the number of ratings by genre  
    print("\nQuestion 3:")
    data_mid_genre = data_g.select('movie_id').join(item_genre, ['movie_id'], how='full')
    data_mid_genre.select(data_mid_genre.columns[1:]).groupBy().sum().show(truncate=False)
    
    # Q4.Print oldest movie with a 5 rating.
    print("\nQuestion 4:")
    data_five = data_g.select('movie_id','rating').filter(data_g.rating == 5)   
    data_mid_date = data_five.join(item_date, ['movie_id'], how='full')
    data_mid_date.filter(data_mid_date.release_date != datetime.min)\
    .orderBy('release_date').limit(1).select(name_map[data_mid_date['movie_id']].alias('movie_name')).show(truncate=False)
    
    # Q5.Print a list of the genre of the top 10 most rated movies. 
    print("\nQuestion 5:")
    top_ten_genres = top_ten_ratings.join(item_genre, ['movie_id'],how='left')
    top_ten_genres_l = top_ten_genres.rdd.map(lambda x: genreReplace(x.asDict(), movie_names))
    for item in top_ten_genres_l.take(10):
        print(item)
    
    # Q6.Print the title of the movie that was rated the most by students 
    print("\nQuestion 6:")
    user_g_data_g = data_g.select('user_id','movie_id').join(user_g, ['user_id'],how='left')
    user_g_data_g.select('movie_id','user_id').filter(user_g_data_g.occupation == 'student').groupBy('movie_id').count()\
    .orderBy('count', ascending=False).limit(1).select(name_map[user_g_data_g['movie_id']].alias('movie_name')).show(truncate=False)
    
    # Q7.Print the list of movies that received the highest number of “5” rating 
    print("\nQuestion 7:")
    data_g.select('movie_id','rating').filter(data_g.rating == 5).groupBy('movie_id').count().orderBy('count', ascending=False).limit(10)\
    .select(name_map[data_g['movie_id']].alias('movie_name')).show(truncate=False)
    
    # Q8.Print the list of zip codes corresponding to the highest number of users that rated movies. 
    print("\nQuestion 8:")
    user_g.select('zip_code').groupBy('zip_code').count().orderBy('count', ascending=False).limit(10).show()
    
    # Q9.Find the most rated movie by users in the age group 20 to 25.
    print("\nQuestion 9:")
    user_g_data_g.select('movie_id','age').filter((user_g_data_g.age <= 25) & (user_g_data_g.age >= 20)).groupBy('movie_id').count()\
    .orderBy('count', ascending=False).limit(1).select(name_map[data_g['movie_id']].alias('movie_name')).show(truncate=False)
    
    # Q10.Print the list of movies that were rated after year 1960. Is this question correct? Should it be "made" after 1960?
    print("\nQuestion 10:")
    data_g.select('movie_id','timestamp').filter(data_g.timestamp >= datetime(1960, 1, 1))\
    .select(name_map[data_g['movie_id']].alias('movie_name')).show(truncate=False)
    
    # Bonus. Convert UnixTimeStamp in timestamp column of u.data to human readable date 
    print("\nBonus Question:")
    data_g.select('timestamp').show(1)

if __name__ == "__main__":
    main()