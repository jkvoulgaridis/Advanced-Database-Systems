from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("rdd-query5").getOrCreate()

sc = spark.sparkContext

hdfs = "hdfs://master:9000/data/"

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

movies = sc.textFile(hdfs + "movies.csv"). \
	    map(lambda x : (split_complex(x)[0], (split_complex(x)[1], float(split_complex(x)[7]))))

genres = sc.textFile(hdfs + "movie_genres.csv"). \
	    map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = sc.textFile(hdfs + "ratings.csv"). \
	     map(lambda x : (x.split(",")[1], (x.split(",")[0], float(x.split(",")[2]))))

result1 = ratings.join(genres). \
		  map(lambda x : ((x[1][0][0], x[1][1]), (x[0], x[1][0][1], 1))). \
		  reduceByKey(lambda x,y : (x[0], x[1], x[2] + y[2])). \
		  map(lambda x : (x[0][1], (x[0][0], x[1][2]))). \
		  groupByKey(). \
		  mapValues(lambda x : sorted(x, key = lambda x : x[2])[-1]). \
		  map(lambda x : (x[1][0], (x[0], x[1][2])))
		  
result2 = ratings.join(movies). \
		  map(lambda x : (x[1][0][0], (x[0], x[1][1][0], x[1][0][1], x[1][1][1])))

result = result1.join(result2). \
		 map(lambda x : ((x[0], x[1][0][0]), (x[1][0][1], x[1][1][0], x[1][1][1], x[1][1][2])))
		 #groupByKey()

#best = result.mapValues(lambda x : sorted(x, key = lambda x : (x[2], x[3]))[-1])

#worst = result.mapValues(lambda x : sorted(x, key = lambda x : (-x[2], x[3]))[-1])

for i in result.collect():
	print(i)

#for i in worst.collect():
#	print(i)

