from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession\
        .builder\
        .master("local")\
        .appName("SocketSource")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

HOST = "127.0.0.1"
PORT = "9999"

# Create Streaming DataFrame by reading data from socket.
initDF = spark\
         .readStream\
         .format("socket")\
         .option("host", HOST)\
         .option("port", PORT)\
         .load()


wordCount = (initDF
  .select(f.explode(f.split(f.col("value"), " ")).alias("words"))
  .groupBy("words")
  .count()
  )

wordCount.writeStream.outputMode("complete").option("truncate", False).format("console").start().awaitTermination()
