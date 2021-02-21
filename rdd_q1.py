from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("rdd-query1").getOrCreate()

sc = spark.sparkContext

hdfs = "hdfs://master:9000/data/"

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

result = sc.textFile(hdfs + "movies.csv"). \
	    map(lambda x : (split_complex(x)[0], (split_complex(x)[1], split_complex(x)[3][:4], \
	                                          float(split_complex(x)[5]), float(split_complex(x)[6])))). \
	    filter(lambda x : x[1][1] != '' and x[1][2] != 0 and x[1][3] != 0). \
	    filter(lambda x : int(x[1][1]) >= 2000). \
	    map(lambda x : (x[1][1], (x[1][0], 100*(x[1][3] - x[1][2])/x[1][2]))). \
	    groupByKey(). \
	    mapValues(lambda x : sorted(x, key = lambda x : x[1])[-1]). \
	    map(lambda x : (x[0], x[1][0], x[1][1]))
