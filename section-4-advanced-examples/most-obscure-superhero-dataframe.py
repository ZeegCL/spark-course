from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///D:/workspace/spark-course/data/Marvel+Names.txt")

lines = spark.read.text("file:///D:/workspace/spark-course/data/Marvel+Graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionsCount = connections.agg(func.min("connections")).first()[0]

mostObscure = connections.filter(func.col("connections") == minConnectionsCount)

mostObscureWithNames = mostObscure.join(names, "id")

print("The following superheroes have the least amount of connections: ")

mostObscureWithNames.select("name", "connections").show(mostObscureWithNames.count())



