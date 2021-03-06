import main
import sys
from pyspark.mllib.linalg.distributed import RowMatrix

if len(sys.argv) >= 3:
    searchKey1 = sys.argv[1]
    searchKey2 = sys.argv[2]
    spark = main.build_session()
    result_user1 = spark.ratings_frame.filter(spark.ratings_frame.userId == searchKey1).drop("userId", "timestamp").withColumnRenamed("rating", "rating1")
    result_user2 = spark.ratings_frame.filter(spark.ratings_frame.userId == searchKey2).drop("userId", "timestamp").withColumnRenamed("rating", "rating2")
    # produces dataframe with both users' ratings for the same film presented alongside each other
    results = result_user1.join(result_user2, "movieId", "outer")
    results.repartition(1).write.format("csv").mode("overwrite").save("moviecolumns.csv", header="true")
    # two main metrics; compare ratings for similar movies and compare amount of moves watched by one user but not the other
    results_rowmatrix = RowMatrix(results.drop("movieId").rdd.map(list))
    print("Degree of error in similarity of movie tastes: {0}".format(str(results_rowmatrix.columnSimilarities())))
else:
    quit(1)
