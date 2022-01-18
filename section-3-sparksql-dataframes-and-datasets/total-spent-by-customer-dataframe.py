from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

schema = StructType([ \
        StructField("customerId", IntegerType(), False), \
        StructField("productId", IntegerType(), False), \
        StructField("amount", FloatType(), False)
    ])

raw = spark.read.schema(schema).csv("file:///D:/workspace/spark-course/data/customer-orders.csv")
customerSpents = raw.select("customerId", "amount")
summary = customerSpents.groupBy("customerId").agg(func.round(func.sum("amount"), 2).alias("total_spent"))
summary.sort("total_spent").show(summary.count())

spark.stop()
