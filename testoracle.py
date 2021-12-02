from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("SparkandOracledbTest").getOrCreate()
df=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@localhost:1521/XE")\
.option("dbtable","test")\
.option("user","simran")\
.option("password","sim")\
.option("driver","oracle.jdbc.driver.OracleDriver")\
.load()


df.show()
