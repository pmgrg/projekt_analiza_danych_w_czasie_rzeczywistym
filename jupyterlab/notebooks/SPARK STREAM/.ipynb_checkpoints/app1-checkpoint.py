from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession\
        .builder\
        .master("local")\
        .appName("RateSource")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

initDF = spark\
        .readStream\
        .format("Rate")\
        .option("rowsPerSecond", 1)\
        .load()

resultDF = initDF\
           .withColumn("result", f.col("value") + f.lit(1))

resultDF.writeStream\
.outputMode("append")\
.option("truncate", False)\
.format("console")\
.start().awaitTermination()