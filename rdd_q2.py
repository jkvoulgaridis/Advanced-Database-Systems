from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd-query2").getOrCreate()

sc = spark.sparkContext

hdfs = "hdfs://master:9000/data/"

result = sc.textFile(hdfs + "ratings.csv"). \
<<<<<<< HEAD
	 map(lambda x : (x.split(',')[0], (float(x.split(',')[2]), 1))). \
	 reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])). \
	 map(lambda x : (x[0], x[1][0] / x[1][1]))
=======
         map(lambda x : (x.split(',')[0], (float(x.split(',')[2]), 1))). \
         reduceByKey(lambda x,y : (x[0] + y[0] , x[1] + y[1])). \
         map(lambda x : (x[0], x[1][0] / x[1][1]))
>>>>>>> 84cca547eda01d38e99740a3b065c05208c9b627

cnt_initial = result.count()

result = result.filter(lambda x : x[1] >= 3)

cnt_final = result.count()

for x in range(100):
        print('RESULT')

print(str(100 * (cnt_final/cnt_initial)) + "%")

for x in range(100):
<<<<<<< HEAD
	print('RESULT')
=======
        print('RESULT')
>>>>>>> 84cca547eda01d38e99740a3b065c05208c9b627
