# CS5052-Spark-Practical
DEPENDENCIES:
Python 3, no specific subversion, though do ensure that PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are set to the same subversion
PySpark 2.0+
Numpy
Hadoop (access to WINUTILS.EXE may also be required)

USAGE INSTRUCTIONS:
 SEARCH:
  Run search.py with two or more additional arguments; the first of which, searchOption, denotes a particular search type and must be one of "userId", "movieId", "title", "genre", "year", "userIdList" or "genreList".
  The second argument will be used as the search term.
  For the two list based searches, all argumepts past the first argument will be compiled into a single list of search terms.
  Any resulting dataframes will be saved to search_results.csv in the same folder.
 LIST:
  Run list.py with two additional arguments: argument 1 must be "top_movies_list" or "top_watched_list" and controls which metric movies are ranked by. Argument 2 denotes how many rows are to be returned.
  Resulting dataframes will be saved to results.csv in the same folder.
 COMPARE:
  Run compare.py with two additional arguments, which will be used as the two userId values to be compared.
  Resulting dataframes will be saved to moviecolumns.csv in the same folder.
  FAVOURITES:
  Run favourite.py with any number of additional arguments, which will be compiled into a list of userIds to be evaluated.
