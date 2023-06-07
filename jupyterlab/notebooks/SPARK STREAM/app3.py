#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app3.py
from pyspark.sql import SparkSession


if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # topic subscription
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "test")
        .load()
    )

    # defining output
    query = raw.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
    query.stop()