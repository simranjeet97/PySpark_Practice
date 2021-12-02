from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("SparkandOracledbTest").getOrCreate()
df=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@{HOSTNAME}:{PORT_NUMBER}/{DATABASE_NAME}")\
.option("dbtable","test")\
.option("user","{USERNAME}")\
.option("password","{PASSWORD}")\
.option("driver","oracle.jdbc.driver.OracleDriver")\
.load()


df.show()
