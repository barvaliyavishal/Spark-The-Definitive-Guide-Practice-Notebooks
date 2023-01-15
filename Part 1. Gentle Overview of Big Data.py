# Databricks notebook source
myRange = spark.range(1000).toDF("number")

# COMMAND ----------

divisBy2 = myRange.where("number % 2 = 0")


# COMMAND ----------

divisBy2.count()

# COMMAND ----------

dbutils.fs.cp("file:///D:/Data Engineer/Spark-The-Definitive-Guide-master/data/flight-data/csv/","/public/book/flight/")

# COMMAND ----------

flightData2015 = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv("/data/flight-data/csv/2015-summary.csv")

# COMMAND ----------

flightData2015.take(3)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)


# COMMAND ----------


