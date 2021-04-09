import sys
import main
from pyspark.sql.functions import *

def list_top_movies(n):
    ratings = spark.ratings_frame
    movies = spark.movie_frame
    movie_rankings = ratings.groupBy("movieId").agg(avg("rating").alias("average")).orderBy(desc("average")).limit(int(n))
    rankings_names = movie_rankings.join(movies, on =['movieId'], how='inner').sort(desc("average")).select("title", "average")
    
    return (rankings_names)

def list_top_watches(n):
    ratings = spark.ratings_frame
    movies = spark.movie_frame
    movie_watches = ratings.groupBy('movieId').count().orderBy(desc('count'))
    movie_watches = movie_watches.limit(int(n))
    watches_names = movie_watches.join(movies, on =['movieId'], how='inner').sort(desc("count")).select("title", "count")
    
    return (watches_names)

switch_listOption = {
    "top_movies_list": list_top_movies,
    "top_watched_list": list_top_watches,
}

if len(sys.argv) >= 3:
    listOption = sys.argv[1]
    n = sys.argv[2]

    spark = main.build_session()
    keepColumns = switch_listOption[listOption]
    search = switch_listOption.get(listOption, lambda: "Invalid list option")
    result = search(n)
    print(result)
    result.repartition(1).write.csv("results.csv")
else: 
    quit(1)
