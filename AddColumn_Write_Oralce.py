from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("SparkandOracledbTest").getOrCreate()
from datetime import datetime
from pyspark.sql.functions import lit

print("Start Reading Data from CSV")
df = spark.read.csv("test.csv", header=True, inferSchema=True)

print("Printing the Data from CSV .... ")
df.show()

print("Printing Schema of Data")
df.printSchema()

print("Adding TimeStamp")
df1 = df1.withColumn("Now", lit(str(datetime.now().strftime("%d-%m-%Y"))))
df1.show()

print("Amending the Oralce Table ... ")
df1.write.format("jdbc").option("driver","oracle.jdbc.driver.OracleDriver")\
.option("url","jdbc:oracle:thin:@127.0.0.1:1521/XE")\
.option("user","simran")\
.option("password","sim").mode("append").option("dbtable","TEMP_DEP").save()

print("Reading again .... ")

df2=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@127.0.0.1:1521/XE")\
.option("dbtable","TEMP_DEP")\
.option("user","simran")\
.option("password","sim")\
.option("driver","oracle.jdbc.driver.OracleDriver")\
.load()

df2.show()