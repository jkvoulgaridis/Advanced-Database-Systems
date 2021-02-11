from pyspark.sql import SparkSession
from io import StringIO 
import csv

spark = SparkSession.builder.appName("rdd-query1").getOrCreate()

sc = spark.sparkContext

hdfs = "hdfs://master:9000/data/"

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]

lines  = sc.textFile(hdfs + "movies.csv").map(lambda x : (tuple(split_complex(x)) , 1))

result = lines.map(lambda x : (x[0][0], x[0][1], x[0][3][:4], float(x[0][5]) , float(x[0][6]))). \
	       filter(lambda x : x[2] != '' and x[3]!=0 and x[4]!=0). \
	       filter(lambda x : int(x[2]) > 2000 ). \
	       map(lambda x : ( x[2]  , (x[1] , 100*(x[4] - x[3])/x[3]) ) ). \
	       groupByKey().\
	       mapValues(lambda x : sorted(x,key = lambda x : x[1])[-1] ). \
	       map(lambda x : (x[0] , x[1][0] , x[1][1]))

for x in result.collect():
	print(x)         
