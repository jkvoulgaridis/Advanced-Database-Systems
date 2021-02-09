from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark import SparkConf

spark = SparkSession.builder.appName('ckeck-parquets').getOrCreate()

sc= spark.sparkContext
sqlContext = SQLContext(sc) 

hadoop_fs = 'hdfs://master:9000/data/' 

df = spark.read.parquet(hadoop_fs +'movies.parquet')
df.show() 


df2 = spark.read.parquet(hadoop_fs +'movie_genres.parquet')
df2.show() 

df3 = spark.read.parquet(hadoop_fs +'ratings.parquet')
df3.show() 
