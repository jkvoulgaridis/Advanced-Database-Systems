from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("rdd-query5").getOrCreate()

sc = spark.sparkContext

hdfs = "hdfs://master:9000/data/"

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

lines = sc.textFile(hdfs + "movies.csv"). \
           map(lambda x : (split_complex(x)[0], tuple(split_complex(x)[1:])))

movies = lines.map(lambda x : (x[0], ()))

genres = sc.textFile(hdfs + "movie_genres.csv"). \

ratings = sc.textFile(hdfs + "ratings.csv"). \