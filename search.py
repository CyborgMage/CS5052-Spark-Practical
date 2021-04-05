import sys
import main
from pyspark.sql.functions import countDistinct, sum, col


#Note: correcting user input is out of scope for this implementation.
#More elegant error handling warranted?

def search_by_userid(search_id):
    result_movies = spark.ratings_frame.filter(spark.ratings_frame.userId == search_id)
    movie_count = result_movies.count()
    #Note: current implementation does not split genre entries
    genre_count = result_movies.join(spark.movie_frame, "movieId").agg(countDistinct("genres")).collect()[0]["count(genres)"]
    return "User {0} watched {1} movies across {2} genres.".format(str(search_id), str(movie_count), str(genre_count))


def search_by_movieid(search_id):
    result_movies = spark.ratings_frame.filter(spark.ratings_frame.movieId + search_id)
    view_count = result_movies.count()
    average_rating = result_movies.agg(sum("rating")).collect()[0][0] / view_count
    return "Movie {0} has an average rating of {1} across {2} viewers.".format(str(search_id), str(average_rating),
                                                                               str(view_count))


def search_by_title(search_id):
    movie_id = spark.movie_frame.filter(spark.movie_frame.title == search_id).collect()[0]["movieId"]
    print("Movie \'{0}\' has code {1}".format(str(search_id), movie_id))
    return search_by_movieid(movie_id)


def search_by_genre(search_id):
    result_movies = spark.movie_frame.filter(col("genres").contains(search_id))
    result_movies.show()
    return ""


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
