from pyspark.sql import SparkSession


def build_session():
    spark = SparkSession.builder.master("local").appName("Movie Ratings").getOrCreate()

    spark.movie_frame = spark.read.format("csv").option("header", "true")\
        .option("mode", "DROPMALFORMED").load("ml-latest-small/movies.csv")

    spark.ratings_frame = spark.read.format("csv").option("header", "true")\
        .option("mode", "DROPMALFORMED").load("ml-latest-small/ratings.csv")

    spark.links_frame = spark.read.format("csv").option("header", "true")\
        .option("mode", "DROPMALFORMED").load("ml-latest-small/links.csv")

    spark.tags_frame = spark.read.format("csv").option("header", "true")\
        .option("mode", "DROPMALFORMED").load("ml-latest-small/tags.csv")

    return spark
