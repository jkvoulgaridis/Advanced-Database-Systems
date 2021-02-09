from pyspark.sql import SparkSession ,  DataFrame , SQLContext
from pyspark import SparkConf
from  io import StringIO
import csv


spark  = SparkSession.builder.appName('load_parquet').getOrCreate() 

sc = spark.sparkContext

sqlContext = SQLContext(sc)

hadoop_fs = 'hdfs://master:9000/data/'

f1 = hadoop_fs + 'movies.csv'
f1_columns = ['id' , 'title', 'summary', 'first_view' , 'duration' , 'cost' , 'revenues', 'popularity']

f2 = hadoop_fs + 'movie_genres.csv' 
f2_columns = ['id' , 'category' ]


f3 = hadoop_fs + 'ratings.csv'
f3_columns = ['person_id' , 'movie_id', 'rating' , 'timestamp']

def mapper(line):
	values = line.split(',')
	return tuple(values)

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]

lines = sc.textFile(f1).map(split_complex)
df = sqlContext.createDataFrame(lines , f1_columns)
df.write.mode('overwrite').parquet(hadoop_fs + 'movies.parquet')

lines = sc.textFile(f2).map(mapper)
df = sqlContext.createDataFrame(lines , f2_columns)
df.write.mode('overwrite').parquet(hadoop_fs + 'movie_genres.parquet')


lines = sc.textFile(f3).map(mapper)
df = sqlContext.createDataFrame(lines , f3_columns)
df.write.mode('overwrite').parquet(hadoop_fs + 'ratings.parquet')

'''
df2 = spark.read.parquet(hadoop_fs + 'genres-parquet')
df2.show()
'''
