import sys
import main
from pyspark.sql.functions import *

def list_top_movies(n):
    ratings = spark.ratings_frame
    movies = spark.movie_frame
    movie_rankings = ratings.join(movies, ratings.movieId == movies.movieId).groupBy(movies.movieId)\
    .agg(avg("rating").alias("average_rating")).orderBy(desc("average_rating")).select("movieId", "average_rating")
    
    return (movie_rankings.take(n))

def list_top_watches(n):
    links = spark.links_frame
    movies = spark.movie_frame
    movie_watches = links.join(movies, links.movieId == movies.movieId).groupBy(movies.movieId)\
    .agg(count("movieId").alias("count")).orderBy(desc("count")).select("movieId", "count")
    
    return (movie_watches.take(n))

switch_listOption = {
    "top_movies_list": list_top_movies,
    "top_watched_list": list_top_watches,
}

if len(sys.argv >= 2):
    listOption = sys.argv[1]
    n = sys.argv[2]

    spark = main.build_session()
    keepColumns = switch_listOption[listOption]
    search = switch_listOption.get(listOption, lambda: "Invalid list option")
    print(search(n))

else: 
    quit(1)
