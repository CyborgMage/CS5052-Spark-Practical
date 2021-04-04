import sys
from pyspark.sql import SparkSession

switch_searchOption = {
    "userId": {"userId", "title", "tag"},
    "movieId": {"userId", "movieId", "title", "rating"},
    "title": {"userId", "movieId", "title", "rating"},
    "tag": {"tag", "title"}
    # Movie titles include year, so base title searching may already cover searching movies by year?
}

if len(sys.argv) >= 4:
    fileLocation = sys.argv[1]
    searchOption = sys.argv[2]
    searchKey = sys.argv[3]
    spark = SparkSession.builder.getOrCreate()
    # TODO: load from file here
    df = spark.read.load(fileLocation)
    # TODO: i/o error handling here
    keepColumns = switch_searchOption[searchOption]
    # TODO: possibly malformed search option argument handling here?
    search = df.filter(searchOption + " like " + searchKey).select(*keepColumns)
    # TODO: additional data wrangling here: possibly convert from list to movie count for user search; average
    # ratings and count watches for movie search
    search.show()
else:
    quit(1)
