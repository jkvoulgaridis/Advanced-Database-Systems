from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q1-sql").getOrCreate()

def get_year(timestamp):
	if timestamp == None:
		return None
	else:
		return timestamp.year 

movies = spark.read.format('csv'). \
	options(header = 'false' , inferSchema='true'). \
	load('hdfs://master:9000/data/movies.csv')

movies.registerTempTable('movies')
#spark.udf.register('year' , get_year)

sqlString = \
	"SELECT " + \
	"m._c1 as Title, 100*(m._c6-m._c5)/m._c5 as Earnings, EXTRACT(YEAR FROM m._c3) as Year " + \
	"FROM movies as m "+\
	"INNER JOIN " +\
	"( " + \
	"SELECT max(100*(_c6-_c5)/_c5) as Earn, EXTRACT(YEAR FROM _c3) as Year "+\
	"from movies " + \
	"WHERE _c3 is not null and _c6 is not null and _c5 is not null and _c5 != 0 "+\
	"group by EXTRACT(YEAR FROM _c3) " +\
	") as MaxProfit "+ \
	"on MaxProfit.Earn = 100*(m._c6-m._c5)/m._c5 and MaxProfit.Year = EXTRACT(YEAR FROM m._c3) " +\
	"order by EXTRACT(YEAR FROM m._c3) DESC"
	

res = spark.sql(sqlString)
res.show()
