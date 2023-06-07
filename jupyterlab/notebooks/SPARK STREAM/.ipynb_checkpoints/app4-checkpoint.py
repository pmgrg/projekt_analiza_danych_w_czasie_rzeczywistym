# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app4.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
)

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # json message schema
    json_schema = StructType(
        [
            StructField("event_time", StringType()),
            StructField("value", FloatType()),
        ]
    )
    
    # topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "test")
        .load()
    )
    
    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("event_time").alias("event_time"),
        f.col("json").getField("value").alias("value"),
    )

    # defining output
    query = parsed.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
    query.stop()
    
    


