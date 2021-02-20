from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('load_parquet').getOrCreate() 

hadoop_fs = 'hdfs://master:9000/data/'

df = spark.read.csv(hadoop_fs + 'movies.csv')
df = df.toDF('id', 'title', 'summary', 'release_date', 'duration', 'cost', 'revenues', 'popularity')
df.write.mode('overwrite').parquet(hadoop_fs + 'movies.parquet')

df = spark.read.csv(hadoop_fs + 'movie_genres.csv')
df = df.toDF('id', 'category')
df.write.mode('overwrite').parquet(hadoop_fs + 'movie_genres.parquet')

df = spark.read.csv(hadoop_fs + 'ratings.csv')
df = df.toDF('person_id', 'movie_id', 'rating', 'timestamp')
df.write.mode('overwrite').parquet(hadoop_fs + 'ratings.parquet')
