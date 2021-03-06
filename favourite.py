import sys
import main
from pyspark.sql.functions import *

def favourite_movies(user_ids):
    movies = spark.movie_frame
    ratings = spark.ratings_frame.filter(col("userId").isin(user_ids))
    user_movies = ratings.join(movies, on =['movieId'], how='inner').select("movieId", "genres")
    genre_dict = {}

    for row in user_movies.rdd.collect():
        genres = row["genres"]
        genres_list = genres.split("|")
        for genre in genres_list:
            if genre in genre_dict:
                x = int(genre_dict.get(genre))
                x = x+1
                genre_dict[genre] = str(x)
            else:
                genre_dict[genre] = "0"

    top_genre = []
    top_Watches = 0

    for genre in genre_dict:
        currentValue = int(genre_dict.get(genre))
        if  currentValue > top_Watches:
            top_genre.clear()
            top_genre.append(genre)
            top_Watches = currentValue
        elif currentValue == top_Watches:
            top_genre.append(genre)
        else:
            continue

    return top_genre

if len(sys.argv) >= 2:
    fav_users = sys.argv[1:]
    spark = main.build_session()
    top_genres = favourite_movies(fav_users)
    print("The favourite genre(s) for that user/group of users is:")
    for genre in top_genres:
        print(genre + "\n")
else: 
    quit(1)
