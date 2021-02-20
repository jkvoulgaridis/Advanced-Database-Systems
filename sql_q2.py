from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q1-sql").getOrCreate()

ratings = spark.read.format('csv'). \
	options(header = 'false' , inferSchema='true'). \
	load('hdfs://master:9000/data/ratings.csv')

ratings.registerTempTable('ratings')
#spark.udf.register('year' , get_year)

sqlString = \
	"Select 100*(  " +\
	"select distinct count(*) from  ( "+\
	"Select r._c0, avg(r._c2) " +\
	"from ratings as r "+\
	"group by r._c0 "+\
	"having avg(r._c2) >= 3.0 ) ) / (select distinct count(_c0) from ratings ) as Percentage "

res = spark.sql(sqlString)
res.show()
