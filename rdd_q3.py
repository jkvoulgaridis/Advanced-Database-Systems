from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd-query3").getOrCreate()

sc = spark.sparkContext

hdfs = "hdfs://master:9000/data/"

genres = sc.textFile(hdfs + "movie_genres.csv"). \
<<<<<<< HEAD
	    map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = sc.textFile(hdfs + "ratings.csv"). \
	     map(lambda x : (x.split(",")[1], (float(x.split(",")[2]), 1))). \
	     reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])). \
	     map(lambda x : (x[0], x[1][0] / x[1][1]))

result = ratings.join(genres). \
		 map(lambda x : (x[1][1], (x[1][0], 1))). \
		 reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])). \
		 map(lambda x : (x[0], x[1][0] / x[1][1], x[1][1]))

for i in result.collect():
	print(i)
=======
            map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = sc.textFile(hdfs + "ratings.csv"). \
             map(lambda x : (x.split(",")[1], (float(x.split(",")[2]), 1))). \
             reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])). \
             map(lambda x : (x[0], x[1][0] / x[1][1]))

result = ratings.join(genres). \
                 map(lambda x : (x[1][1], (x[1][0], 1))). \
                 reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])). \
                 map(lambda x : (x[0], x[1][0] / x[1][1], x[1][1]))

for i in result.collect():
        print(i)
>>>>>>> 84cca547eda01d38e99740a3b065c05208c9b627
