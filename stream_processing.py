from pyspark.sql.functions import col, current_timestamp, unix_timestamp

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import (
    from_json, col, window, avg, count, current_timestamp,
    expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType, ArrayType,IntegerType
)
from threading import Thread
import time
import json
import shutil, os
from pyspark.sql.functions import explode

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Global Mart Stream Processing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.streaming.checkpointLocation", "file:///tmp/global-mart-stream") \
        .getOrCreate()
def define_user_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("preferences", ArrayType(StringType()), True)
    ])
def define_product_schema():
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("inventory", IntegerType(), True),
        StructField("category", StringType(), True)
    ])
def define_cart_product_schema():
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])
def define_view_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
def define_cart_schema():
    return StructType([
        StructField("cart_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("products", ArrayType(define_cart_product_schema()), True)
    ])
def define_transaction_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("products", ArrayType(define_cart_product_schema()), True)
    ])
def store_data_disk():
    print("\n" + "="*60)
    print("Starting Spark Streaming Backend")
    print("="*60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    kafka_user_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "globalmart.users")
         .option("kafka.group.id", "globalmart-user-streamer")
         .option("startingOffsets", "latest")
         .load()
    )
    user_schema = define_user_schema()
    # Parse JSON
    parsed_user_df = kafka_user_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), user_schema).alias("data")) \
        .select("data.*")
    
    # Write to memory table for querying
    user_writer = (parsed_user_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", "file:///home/m/Desktop/bigdata_proj/Staging/users/")
        .option("checkpointLocation", "file:///home/m/Desktop/bigdata_proj/Staging/checkpoints/users/")
        .start()
        )
    
    kafka_product_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "globalmart.product_catalog")
         .option("kafka.group.id", "globalmart-product-streamer")
         .option("startingOffsets", "latest")
         .load()
    )
    product_schema = define_product_schema()
    # Parse JSON
    parsed_product_df = kafka_product_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), product_schema).alias("data")) \
        .select("data.*")
    
    # Write to memory table for querying
    product_writer = (parsed_product_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", "file:///home/m/Desktop/bigdata_proj/Staging/products/")
        .option("checkpointLocation", "file:///home/m/Desktop/bigdata_proj/Staging/checkpoints/products/")
        .start()
        )
    
    kafka_view_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "globalmart.product_views")
         .option("kafka.group.id", "globalmart-product-view-streamer")
         .option("startingOffsets", "latest")
         .load()
    )
    view_schema = define_view_schema()
    # Parse JSON
    parsed_view_df = kafka_view_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), view_schema).alias("data")) \
        .select("data.*")
    
    # Write to memory table for querying
    views_writer = (parsed_view_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", "file:///home/m/Desktop/bigdata_proj/Staging/views/")
        .option("checkpointLocation", "file:///home/m/Desktop/bigdata_proj/Staging/checkpoints/views/")
        .start()
        )
    
    kafka_cart_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "globalmart.cart_events")
         .option("kafka.group.id", "globalmart-cart-streamer")
         .option("startingOffsets", "latest")
         .load()
    )
    #FOR THE CART, YOU NEED TO HANDLE HAVING THE SAME PRODUCT MULTIPLE TIMES!!!!!!!!!!!!!!
    cart_schema = define_cart_schema()
    # Parse JSON
    parsed_cart_df = kafka_cart_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), cart_schema).alias("data")) \
        .select("data.*")
    
    # Write to memory table for querying
    cart_writer = (parsed_cart_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", "file:///home/m/Desktop/bigdata_proj/Staging/carts/")
        .option("checkpointLocation", "file:///home/m/Desktop/bigdata_proj/Staging/checkpoints/carts/")
        .start()
        )
    
    kafka_transaction_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "globalmart.transaction_events")
         .option("kafka.group.id", "globalmart-transaction-streamer")
         .option("startingOffsets", "latest")
         .load()
    )
    transaction_schema = define_transaction_schema()
    # Parse JSON
    parsed_transaction_df = kafka_transaction_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), transaction_schema).alias("data")) \
        .select("data.*")
    
    # Write to memory table for querying
    transaction_writer = (parsed_transaction_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", "file:///home/m/Desktop/bigdata_proj/Staging/transactions/")
        .option("checkpointLocation", "file:///home/m/Desktop/bigdata_proj/Staging/checkpoints/transactions/")
        .start()
        )
    
    print("\n✓ Streaming To Disk started")
    print("  - Spark UI: http://localhost:4040")
    user_writer.awaitTermination(10000)
    product_writer.awaitTermination(10000)
    views_writer.awaitTermination(10000)
    cart_writer.awaitTermination(10000)
    transaction_writer.awaitTermination(10000)

if __name__ == "__main__":
    try:
        CHECKPOINT_DIR = "/tmp/global-mart-stream"
        if os.path.exists(CHECKPOINT_DIR):
            print(f"🧹 Removing old checkpoint dir: {CHECKPOINT_DIR}")
            shutil.rmtree(CHECKPOINT_DIR)
        # Start Spark streaming in background thread
        # spark_thread = Thread(target=store_data_disk)
        # spark_thread.start()
        store_data_disk()
        # Give Spark time to start
        #time.sleep(5)
        
    except KeyboardInterrupt:
        print("\n\nShutting down dashboard...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
