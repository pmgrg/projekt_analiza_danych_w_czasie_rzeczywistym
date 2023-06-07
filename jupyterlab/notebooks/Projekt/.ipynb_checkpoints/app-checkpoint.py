# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 app.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType
)

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/jovyan/notebooks/Projekt/checkpoint")
    
    # json message schema
    json_schema = StructType(
        [
            StructField("ID", IntegerType()),
            StructField("Year_Birth", IntegerType()),
            StructField("Education", StringType()),
            StructField("Marital_Status", StringType()),
            StructField("Income", IntegerType()),
            StructField("Kidhome", IntegerType()),
            StructField("Teenhome", IntegerType()),
            StructField("Dt_Customer", StringType()),
            StructField("Recency", IntegerType()),
            StructField("MntWines", IntegerType()),
            StructField("MntFruits", IntegerType()),
            StructField("MntMeatProducts", IntegerType()),
            StructField("MntFishProducts", IntegerType()),
            StructField("MntSweetProducts", IntegerType()),
            StructField("MntGoldProds", IntegerType()),
            StructField("NumDealsPurchases", IntegerType()),
            StructField("NumWebPurchases", IntegerType()),
            StructField("NumCatalogPurchases", IntegerType()),
            StructField("NumStorePurchases", IntegerType()),
            StructField("NumWebVisitsMonth", IntegerType()),
            StructField("AcceptedCmp3", IntegerType()),
            StructField("AcceptedCmp4", IntegerType()),
            StructField("AcceptedCmp5", IntegerType()),
            StructField("AcceptedCmp1", IntegerType()),
            StructField("AcceptedCmp2", IntegerType()),
            StructField("Complain", IntegerType()),
            StructField("Z_CostContact", IntegerType()),
            StructField("Z_Revenue", IntegerType()),
            StructField("Response", IntegerType()),
        ]
    )
    
    # topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "table")
        .load()
    )
    
    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("ID").alias("ID"),
        f.col("json").getField("Year_Birth").alias("Year_Birth"),
        f.col("json").getField("Education").alias("Education"),
        f.col("json").getField("Marital_Status").alias("Marital_Status"),
        f.col("json").getField("Income").alias("Income"),
        f.col("json").getField("Kidhome").alias("Kidhome"),
        f.col("json").getField("Teenhome").alias("Teenhome"),
        f.col("json").getField("Dt_Customer").alias("Dt_Customer"),
        f.col("json").getField("Recency").alias("Recency"),
        f.col("json").getField("MntWines").alias("MntWines"),
        f.col("json").getField("MntFruits").alias("MntFruits"),
        f.col("json").getField("MntMeatProducts").alias("MntMeatProducts"),
        f.col("json").getField("MntFishProducts").alias("MntFishProducts"),
        f.col("json").getField("MntSweetProducts").alias("MntSweetProducts"),
        f.col("json").getField("MntGoldProds").alias("MntGoldProds"),
        f.col("json").getField("NumDealsPurchases").alias("NumDealsPurchases"),
        f.col("json").getField("NumWebPurchases").alias("NumWebPurchases"),
        f.col("json").getField("NumCatalogPurchases").alias("NumCatalogPurchases"),
        f.col("json").getField("NumStorePurchases").alias("NumStorePurchases"),
        f.col("json").getField("NumWebVisitsMonth").alias("NumWebVisitsMonth"),
        f.col("json").getField("AcceptedCmp3").alias("AcceptedCmp3"),
        f.col("json").getField("AcceptedCmp4").alias("AcceptedCmp4"),
        f.col("json").getField("AcceptedCmp5").alias("AcceptedCmp5"),
        f.col("json").getField("AcceptedCmp1").alias("AcceptedCmp1"),
        f.col("json").getField("AcceptedCmp2").alias("AcceptedCmp2"),
        f.col("json").getField("Complain").alias("Complain"),
        f.col("json").getField("Z_CostContact").alias("Z_CostContact"),
        f.col("json").getField("Z_Revenue").alias("Z_Revenue"),
        f.col("json").getField("Response").alias("Response")       
    )

    current_directory = os.getcwd()
    print("Current directory:", current_directory)


    query = parsed.writeStream.outputMode("append").format("parquet").option("path", "/home/jovyan/notebooks/Projekt/data").start()
    
    # defining output
    #query = parsed.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
    query.stop()
    
    


