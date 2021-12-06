from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("SparkandOracledbTest").getOrCreate()

df1=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@127.0.0.1:1521/XE")\
.option("dbtable","TEMP_DEP")\
.option("user","simran")\
.option("password","sim")\
.option("driver","oracle.jdbc.driver.OracleDriver")\
.load()

df1.show()
