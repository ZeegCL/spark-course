from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAgeDataframe").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///D:/workspace/spark-course/data/fakefriends-header.csv")
    
friendsByAge = people.select("age", "friends")
friendsByAge.groupBy("age").agg(
    func.round(func.avg("friends"), 2).alias("avg_friends")
    ).orderBy("age").show()