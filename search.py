import sys
import main
from pyspark.sql.functions import countDistinct


def search_by_userid(search_id):
    result_movies = spark.ratings_frame.filter("userId LIKE " + search_id)
    movie_count = result_movies.count()
    #Note: current implementation does not split genre entries
    genre_count = result_movies.join(spark.movie_frame, "movieId").agg(countDistinct("genres")).collect()[0]["count(genres)"]
    return "User " + str(search_id) + " watched " + str(movie_count) + " movies across " + str(genre_count) + " genres."


def search_by_movieid(search_id):
    print()


def search_by_title(search_id):
    print()


def search_by_genre(search_id):
    print()


switch_searchOption = {
    "userId": search_by_userid,
    "movieId": search_by_movieid,
    "title": search_by_title,
    "genre": search_by_genre,
    # Movie titles include year, so base title searching may already cover searching movies by year?
}


if len(sys.argv) >= 3:
    searchOption = sys.argv[1]
    searchKey = sys.argv[2]
    spark = main.build_session()
    keepColumns = switch_searchOption[searchOption]
    search = switch_searchOption.get(searchOption, lambda: "Invalid search option")
    print(search(searchKey))
else:
    quit(1)
