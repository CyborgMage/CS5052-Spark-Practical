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
    print("User {0} watched {1} movies across {2} genres.".format(str(search_id), str(movie_count), str(genre_count)))
    return result_movies


def search_by_movieid(search_id):
    result_movies = spark.ratings_frame.filter(spark.ratings_frame.movieId == search_id)
    view_count = result_movies.count()
    average_rating = result_movies.agg(sum("rating")).collect()[0][0] / view_count
    print("Movie {0} has an average rating of {1} across {2} viewers.".format(str(search_id), str(average_rating),
                                                                               str(view_count)))
    return result_movies

def search_by_title(search_id):
    movie_id = spark.movie_frame.filter(spark.movie_frame.title == search_id).collect()[0]["movieId"]
    print("Movie \'{0}\' has code {1}".format(str(search_id), movie_id))
    return search_by_movieid(movie_id)


def search_by_genre(search_id):
    result_movies = spark.movie_frame.filter(col("genres").contains(search_id))
    return result_movies

def search_by_year(search_id):
    result_movies = spark.movie_frame.filter(col("titles").contains(search_id))
    return result_movies

def search_by_useridlist(search_ids):
    movie_ids = spark.ratings_frame.filter(col("userId").isin(search_ids))
    result_movies = spark.movie_frame.join(movie_ids, spark.movie_frame.movieId == movie_ids.movieId, how="leftsemi")
    return result_movies


def search_by_genrelist(search_ids):
    entry_results = []
    for x in search_ids:
        entry_results.append(spark.movie_frame.filter(col("genres").contains(x)))
    list_results = entry_results[0]
    for y in entry_results[1:]:
        list_results = list_results.union(y)
    #Ordering necessary here? Was included here to help confirm correct behaviour. Appears to order movieId as string, not number value.
    return list_results.dropDuplicates().orderBy("movieId")


switch_searchOption = {
    "userId": search_by_userid,
    "movieId": search_by_movieid,
    "title": search_by_title,
    "genre": search_by_genre,
    "year": search_by_year,
    "userIdList" : search_by_useridlist,
    "genreList" :search_by_genrelist
    # Movie titles include year, so base title searching may already cover searching movies by year?
}


if len(sys.argv) >= 3:
    searchOption = sys.argv[1]
    if len(sys.argv) == 3:
        searchKey = sys.argv[2]
    else:
        searchKey = sys.argv[2:]
    spark = main.build_session()
    keepColumns = switch_searchOption[searchOption]
    search = switch_searchOption.get(searchOption, lambda: "Invalid search option")
    search(searchKey).write.format("csv").mode("overwrite").save("searchresult.csv", header="true")
    print("Output saved to csv")
else:
    quit(1)
