from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local")
.appName("Movie Ratings") 
.getOrCreate()

movie_frame = spark.read.format("csv")
.option("header", "true") 
.option("mode", "DROPMALFORMED")
.load("ml-latest-small/movies.csv")

ratings_frame = spark.read.format("csv")
.option("header", "true")
.option("mode", "DROPMALFORMED")
.load("ml-latest-small/ratings.csv")

links_frame = spark.read.format("csv")
.option("header", "true") 
.option("mode", "DROPMALFORMED")
.load("ml-latest-small/links.csv")

tags_frame = spark.read.format("csv")
.option("header", "true")
.option("mode", "DROPMALFORMED")
.load("ml-latest-small/tags.csv")
