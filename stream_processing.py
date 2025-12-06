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
    expr, lit, to_json, struct
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

from config import KAFKA_CONFIG, PATHS, SPARK_CONFIG, ANOMALY_DETECTION, ALERT_CONFIG
from data_quality import (
    validate_users,
    validate_products,
    validate_views,
    validate_carts,
    validate_transactions
)
from real_time_analytics import (
    detect_transaction_anomalies,
    detect_low_inventory,
    generate_transaction_alerts,
    generate_inventory_alerts,
    aggregate_sales_by_hour,
    aggregate_sales_by_category,
    aggregate_sales_by_country
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

# Helper function to write metrics to Kafka
def write_to_kafka(df, topic_name):
    """
    Write DataFrame to Kafka topic as JSON
    """
    # Select all columns and convert to JSON
    json_df = df.select(to_json(struct(*[col(c) for c in df.columns])).alias("value"))

    # Write to Kafka
    json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", topic_name) \
        .save()

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
    """Process product data batch with inventory monitoring"""
    if batch_df.count() > 0:
        # Apply data quality validation
        clean_df, quarantine_df = validate_products(batch_df)

        # Write clean data
        clean_df.write.mode("append").parquet(PATHS['clean_products'])

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_products'])
            print(f"[Batch {batch_id}] Products: {quarantine_df.count()} records quarantined")

        # Detect low inventory on clean data
        inventory_df = detect_low_inventory(clean_df)

        # Generate alerts for low inventory
        generate_inventory_alerts(inventory_df)

        # Write inventory alerts to Kafka stream
        write_to_kafka(inventory_df, KAFKA_CONFIG['topics']['metrics_inventory_alerts'])

        # Count alerts for logging
        alert_count = inventory_df.filter(col("needs_alert") == True).count()
        if alert_count > 0:
            print(f"[Batch {batch_id}] Products: {alert_count} inventory alerts generated and sent to Kafka")

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
    """Process transaction data batch with anomaly detection"""
    if batch_df.count() > 0:
        # Apply data quality validation
        clean_df, quarantine_df = validate_transactions(batch_df)

        # Write clean data
        clean_df.write.mode("append").parquet(PATHS['clean_transactions'])

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_transactions'])
            print(f"[Batch {batch_id}] Transactions: {quarantine_df.count()} records quarantined")

        # Detect transaction anomalies on clean data
        anomaly_df = detect_transaction_anomalies(clean_df)

        # Generate alerts for detected anomalies
        generate_transaction_alerts(anomaly_df)

        # Write anomaly data to Kafka stream
        write_to_kafka(anomaly_df, KAFKA_CONFIG['topics']['metrics_anomalies'])

        # Count anomalies for logging
        anomaly_count = anomaly_df.filter(col("is_anomaly") == True).count()
        if anomaly_count > 0:
            print(f"[Batch {batch_id}] Transactions: {anomaly_count} anomalies detected and sent to Kafka")

        print(f"[Batch {batch_id}] Transactions: {clean_df.count()} clean records written")

# Streaming Analytics Functions - process clean data and generates real-time metrics, writes to Kafka topics

def process_sales_aggregation_batch(batch_df, batch_id):
    """Process sales aggregation for each batch"""
    if batch_df.count() > 0:
        # Read clean product and user data for enrichment
        try:
            product_df = batch_df.sparkSession.read.parquet(PATHS['clean_products'])
            user_df = batch_df.sparkSession.read.parquet(PATHS['clean_users'])
        except:
            product_df = None
            user_df = None
            print(f"[Batch {batch_id}] Sales Aggregation: No product/user data available yet")

        # Aggregate by hour
        hourly_metrics = aggregate_sales_by_hour(batch_df)
        # Add processing timestamp for time-based windowing
        hourly_metrics = hourly_metrics.withColumn("processed_at", current_timestamp())

        # Write to Kafka stream
        write_to_kafka(hourly_metrics, KAFKA_CONFIG['topics']['metrics_sales_hourly'])
        print(f"[Batch {batch_id}] Sales Aggregation: Hourly metrics sent to Kafka")

        # Aggregate by category (if product data available)
        if product_df is not None:
            category_metrics = aggregate_sales_by_category(batch_df, product_df)
            # Add processing timestamp for time-based windowing
            category_metrics = category_metrics.withColumn("processed_at", current_timestamp())

            # Write to Kafka stream
            write_to_kafka(category_metrics, KAFKA_CONFIG['topics']['metrics_sales_category'])
            print(f"[Batch {batch_id}] Sales Aggregation: Category metrics sent to Kafka")

        # Aggregate by country/region (if user data available)
        if user_df is not None:
            country_metrics = aggregate_sales_by_country(batch_df, user_df)
            # Add processing timestamp for time-based windowing
            country_metrics = country_metrics.withColumn("processed_at", current_timestamp())

            # Write to Kafka stream
            write_to_kafka(country_metrics, KAFKA_CONFIG['topics']['metrics_sales_country'])
            print(f"[Batch {batch_id}] Sales Aggregation: Country metrics sent to Kafka")

def process_cart_abandonment_batch(cart_batch_df, batch_id):
    """Process cart abandonment detection - simplified batch approach"""
    if cart_batch_df.count() > 0:
        try:
            from pyspark.sql.functions import explode, sum as spark_sum

            # Calculate cart values
            cart_with_value = cart_batch_df.withColumn("product", explode("products")) \
                .withColumn("item_total", col("product.price") * col("product.quantity")) \
                .groupBy("cart_id", "user_id", "timestamp") \
                .agg(spark_sum("item_total").alias("cart_value"))

            # Filter high-value carts (threshold from config)
            high_value_carts = cart_with_value.filter(col("cart_value") >= ANOMALY_DETECTION['cart_abandonment']['min_cart_value'])

            if high_value_carts.count() > 0:
                # Write to Kafka stream
                write_to_kafka(high_value_carts, KAFKA_CONFIG['topics']['metrics_abandoned_carts'])
                print(f"[Batch {batch_id}] Cart Tracking: {high_value_carts.count()} high-value carts sent to Kafka")
        except Exception as e:
            print(f"[Batch {batch_id}] Cart Tracking: Error - {e}")

# Main Streaming Pipeline
def start_streaming_pl():
    print("\n" + "="*60)
    print("Starting Spark Streaming Backend")
    print("="*60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # =========== Read Streams for Batch Processing ===========
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


    # =========== Publish Real-time Analytics Streams ===========
    # Sales Aggregation
    # Process clean transactions for sales metrics (hourly, category, country)
    sales_aggregation_writer = (parsed_transaction_df
        .writeStream
        .outputMode("append")
        .foreachBatch(process_sales_aggregation_batch)
        .option("checkpointLocation", PATHS['checkpoint_sales_aggregation'])
        .start()
        )

    # Cart Abandonment Detection
    # Process carts and detect abandonment using transaction data
    cart_abandonment_writer = (parsed_cart_df
        .writeStream
        .outputMode("append")
        .foreachBatch(process_cart_abandonment_batch)
        .option("checkpointLocation", PATHS['checkpoint_cart_abandonment'])
        .start()
        )
    

    print("\n✓ Streaming Pipeline Started")
    print("  - Data Quality Streams: Users, Products, Views, Carts, Transactions")
    print("  - Analytics Streams: Sales Aggregation, Cart Abandonment")
    print("  - Spark UI: http://localhost:4040")

    user_writer.awaitTermination(10000)
    product_writer.awaitTermination(10000)
    views_writer.awaitTermination(10000)
    cart_writer.awaitTermination(10000)
    transaction_writer.awaitTermination(10000)
    sales_aggregation_writer.awaitTermination(10000)
    cart_abandonment_writer.awaitTermination(10000)

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

