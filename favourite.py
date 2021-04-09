import sys
import main
from pyspark.sql.functions import *

def favourite_movies(user_ids):
    movies = spark.movies_frame
    ratings = spark.ratings_frame.filter(col("userId").isin(user_ids))
    user_movies = ratings.join(movies, movies.movieId == ratings.movieId).select("movieId", "genres")
    genre_dict = {}

    for row in user_movies.rdd.collect():
        genres = row["genres"]
        genres_list = genres.split("|")
        for genre in genres_list:
            if genre in genre_dict.keys:
                x = int(genre_dict.get(genre))
                x = x+1
                genre_dict[genre] = str(x)
            else:
                genre_dict[genre] = "0"
    
    top_genre = []
    top_Watches = 0

    for genre in genre_dict:
        if genre_dict.get(genre) > top_Watches:
            




if len(sys.argv >= 1):
    fav_users = sys.argv[0:]
    spark = main.build_session()
    print(favourite_movies(fav_users))
else: 
    quit(1)