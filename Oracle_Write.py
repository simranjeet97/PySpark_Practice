from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("SparkandOracledbTest").getOrCreate()

print("Start Reading Data from CSV")
df = spark.read.csv("test.csv", header=True, inferSchema=True)

print("Printing the Data from CSV .... ")
df.show()

print("Printing Schema of Data")
df.printSchema()

print("Start Writing Data to Oracle ...")
print("Creating Table ... ")
print("Table Creation Done")

df.write.format("jdbc").option("driver","oracle.jdbc.driver.OracleDriver")\
.option("url","jdbc:oracle:thin:@127.0.0.1:1521/XE")\
.option("user","simran")\
.option("password","sim").mode("overwrite").option("createTableOptions","")\
.option("dbtable","TEMP_DEP").save()


print("Writing Data to Oracle Completed Successfully .... !")
