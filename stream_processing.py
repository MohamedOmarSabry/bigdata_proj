import os
import sys

# Set SPARK_HOME to use PySpark from virtual environment
pyspark_path = os.path.join(sys.prefix, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages', 'pyspark')
os.environ['SPARK_HOME'] = pyspark_path

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

from config import KAFKA_CONFIG, PATHS, SPARK_CONFIG
from data_quality import (
    validate_users,
    validate_products,
    validate_views,
    validate_carts,
    validate_transactions
)

# Schema Definitions
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

# Create Spark Session

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName(SPARK_CONFIG['app_name']) \
        .config("spark.jars.packages", SPARK_CONFIG['packages']) \
        .config("spark.sql.shuffle.partitions", SPARK_CONFIG['shuffle_partitions']) \
        .getOrCreate()

# Processing Functions - read from Kafka, validate using functions from data_quality, then write to disk in designated folders

def process_user_batch(batch_df, batch_id):
    """Process user data batch"""
    if batch_df.count() > 0:
        # Apply data quality validation
        clean_df, quarantine_df = validate_users(batch_df)

        # Write clean data
        clean_df.write.mode("append").parquet(PATHS['clean_users'])

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_users'])
            print(f"[Batch {batch_id}] Users: {quarantine_df.count()} records quarantined")

        print(f"[Batch {batch_id}] Users: {clean_df.count()} clean records written")

def process_product_batch(batch_df, batch_id):
    """Process product data batch"""
    if batch_df.count() > 0:
        # Apply data quality validation
        clean_df, quarantine_df = validate_products(batch_df)

        # Write clean data
        clean_df.write.mode("append").parquet(PATHS['clean_products'])

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_products'])
            print(f"[Batch {batch_id}] Products: {quarantine_df.count()} records quarantined")

        print(f"[Batch {batch_id}] Products: {clean_df.count()} clean records written")

def process_view_batch(batch_df, batch_id):
    """Process product view data batch"""
    if batch_df.count() > 0:
        # Apply data quality validation
        clean_df, quarantine_df = validate_views(batch_df)

        # Write clean data
        clean_df.write.mode("append").parquet(PATHS['clean_views'])

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_views'])
            print(f"[Batch {batch_id}] Views: {quarantine_df.count()} records quarantined")

        print(f"[Batch {batch_id}] Views: {clean_df.count()} clean records written")

def process_cart_batch(batch_df, batch_id):
    """Process cart data batch"""
    if batch_df.count() > 0:
        # Apply data quality validation
        clean_df, quarantine_df = validate_carts(batch_df)

        # Write clean data
        clean_df.write.mode("append").parquet(PATHS['clean_carts'])

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_carts'])
            print(f"[Batch {batch_id}] Carts: {quarantine_df.count()} records quarantined")

        print(f"[Batch {batch_id}] Carts: {clean_df.count()} clean records written")
    
def process_transaction_batch(batch_df, batch_id):
    """Process transaction data batch"""
    if batch_df.count() > 0:
        # Apply data quality validation
        clean_df, quarantine_df = validate_transactions(batch_df)

        # Write clean data
        clean_df.write.mode("append").parquet(PATHS['clean_transactions'])

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_transactions'])
            print(f"[Batch {batch_id}] Transactions: {quarantine_df.count()} records quarantined")

        print(f"[Batch {batch_id}] Transactions: {clean_df.count()} clean records written")

# Main Streaming Pipeline
def start_streaming_pl():
    print("\n" + "="*60)
    print("Starting Spark Streaming Backend")
    print("="*60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # User Stream
    kafka_user_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers'])
         .option("subscribe", KAFKA_CONFIG['topics']['users'])
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
        .foreachBatch(process_user_batch) # validates and writes to disk
        .option("checkpointLocation", PATHS['checkpoint_users'])
        .start()
        )
    
    # Product Stream
    
    kafka_product_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers'])
         .option("subscribe", KAFKA_CONFIG['topics']['products'])
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
        .foreachBatch(process_product_batch) # validates and writes to disk
        .option("checkpointLocation", PATHS['checkpoint_products'])
        .start()
        )
    
    # Product View Stream
    kafka_view_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers'])
         .option("subscribe", KAFKA_CONFIG['topics']['views'])
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
        .foreachBatch(process_view_batch) # validates and writes to disk
        .option("checkpointLocation", PATHS['checkpoint_views'])
        .start()
        )
    
    # Cart Stream
    kafka_cart_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers'])
         .option("subscribe", KAFKA_CONFIG['topics']['carts'])
         .option("kafka.group.id", "globalmart-cart-streamer")
         .option("startingOffsets", "latest")
         .load()
    )


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
        .foreachBatch(process_cart_batch) # validates and writes to disk
        .option("checkpointLocation", PATHS['checkpoint_carts'])
        .start()
        )
    
    # Transaction Stream
    kafka_transaction_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers'])
         .option("subscribe", KAFKA_CONFIG['topics']['transactions'])
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
        .foreachBatch(process_transaction_batch) # validates and writes to disk
        .option("checkpointLocation", PATHS['checkpoint_transactions'])
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
        # Clean old checkpoints if they exist
        CHECKPOINT_DIR = PATHS['checkpoints']
        if os.path.exists(CHECKPOINT_DIR):
            print(f"Removing old checkpoint dir: {CHECKPOINT_DIR}")
            shutil.rmtree(CHECKPOINT_DIR)

        # Start the streaming pipeline
        start_streaming_pl()

    except KeyboardInterrupt:
        print("\n\n⚠ Shutting down stream processing...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

