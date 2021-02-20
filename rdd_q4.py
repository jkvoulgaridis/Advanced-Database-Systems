from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("rdd-query4").getOrCreate()

sc = spark.sparkContext

hdfs = "hdfs://master:9000/data/"

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

def five_years(movie_year):
	period = ""
	if (int(movie_year) >= 2000 and int(movie_year) <= 2004):
		period = "2000-2004"
	elif (int(movie_year) >= 2005 and int(movie_year) <= 2009):
		period = "2005-2009"
	elif (int(movie_year) >= 2010 and int(movie_year) <= 2014):
		period = "2010-2014"
	elif (int(movie_year) >= 2015 and int(movie_year) <= 2019):
		period = "2015-2019"
	return period

def summary_len(summary):
	return len(summary.split())

movies = sc.textFile(hdfs + "movies.csv"). \
            map(lambda x : (split_complex(x)[0], (split_complex(x)[2], split_complex(x)[3][:4]))). \
	    filter(lambda x : x[1][1] != ''). \
	    filter(lambda x : int(x[1][1]) >= 2000)

genres = sc.textFile(hdfs + "movie_genres.csv"). \
	    map(lambda x : (x.split(",")[0], x.split(",")[1]))

result = movies.join(genres). \
	        map(lambda x : (x[0], (x[1][0][0], x[1][0][1], x[1][1]))). \
		filter(lambda x : x[1][2] == 'Drama'). \
		map(lambda x : (five_years(x[1][1]), (summary_len(x[1][0]), 1))). \
		reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])). \
		map(lambda x : (x[0], x[1][0] / x[1][1]))

for i in result.collect():
	print(i)

