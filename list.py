import sys
import main
from pyspark.sql.functions import *

def list_top_movies(n):
    ratings = spark.ratings_frame
    movies = spark.movie_frame
    movie_rankings = ratings.groupBy("movieId").agg(avg("rating").alias("average")).orderBy(desc("average")).limit(n)
    rankings_names = movie_rankings.join(movies, movie_rankings.movieId == movies.movieId).select("movieId", "average")
    
    return (rankings_names)

def list_top_watches(n):
    ratings = spark.ratings_frame
    movies = spark.movie_frame
    movie_watches = ratings.groupBy('movieId').count().orderBy(desc('count')).limit(n)
    watches_names = movie_watches.join(movies, movie_watches.movieId == movies.movieId).select("movieId", "count")
    
    return (watches_names)

switch_listOption = {
    "top_movies_list": list_top_movies,
    "top_watched_list": list_top_watches,
}

if len(sys.argv >= 2):
    listOption = sys.argv[0]
    n = sys.argv[1]

    spark = main.build_session()
    keepColumns = switch_listOption[listOption]
    search = switch_listOption.get(listOption, lambda: "Invalid list option")
    print(search(n))
else: 
    quit(1)
