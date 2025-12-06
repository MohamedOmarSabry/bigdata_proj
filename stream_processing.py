"""
Main Spark Structured Streaming application organized into three parts:

Data Ingestion, Validation & Staging
    - Reads raw data from Kafka topics
    - Validates using data_quality.py
    - Writes clean data to staging area (Parquet)
    - Quarantines invalid records

Real-Time Metrics Aggregation
    - Aggregates sales by hour, category, and country
    - Publishes metrics to Kafka for dashboard consumption

Anomaly Detection & Alerts
    - Transaction anomaly detection
    - Low inventory alerts
    - Cart abandonment detection
    - Session Analysis
"""

import os
import sys

# Set SPARK_HOME to use PySpark from virtual environment
pyspark_path = os.path.join(sys.prefix, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages', 'pyspark')
os.environ['SPARK_HOME'] = pyspark_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_json, struct, to_timestamp, lit,
    explode, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, ArrayType, IntegerType
)
import shutil

from config import KAFKA_CONFIG, PATHS, SPARK_CONFIG, ANOMALY_DETECTION

# Data Quality imports (Pipeline A)
from data_quality import (
    validate_users,
    validate_products,
    validate_views,
    validate_carts,
    validate_transactions
)

# Real-Time Analytics imports (Pipeline B & C)
from real_time_analytics import (
    # Anomaly detection
    detect_transaction_anomalies,
    detect_low_inventory,
    # Alert generation
    generate_transaction_alerts,
    generate_inventory_alerts,
    # Sales aggregation
    aggregate_sales_by_hour,
    aggregate_sales_by_category,
    aggregate_sales_by_country
)

# Session Analysis imports
from session_analysis import process_sessions_batch



# SCHEMA DEFINITIONS

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


# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(SPARK_CONFIG['app_name']) \
        .config("spark.jars.packages", SPARK_CONFIG['packages']) \
        .config("spark.sql.shuffle.partitions", SPARK_CONFIG['shuffle_partitions']) \
        .getOrCreate()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def write_to_kafka(df, topic_name):
    """
    Write DataFrame to Kafka topic as JSON.
    Used by metrics and alert pipelines to publish to dashboard topics.
    """
    json_df = df.select(to_json(struct(*[col(c) for c in df.columns])).alias("value"))
    json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", topic_name) \
        .save()


# =============================================================================
# DATA INGESTION, VALIDATION & STAGING
# =============================================================================

def ingest_users_batch(batch_df, batch_id):
    """
    Validates user records and writes clean/quarantine Parquet files.
    """
    if batch_df.count() > 0:
        clean_df, quarantine_df = validate_users(batch_df)

        # Write clean data to staging
        clean_df.write.mode("append").parquet(PATHS['clean_users'])
        print(f"[Batch {batch_id}] Users Staging: {clean_df.count()} clean records written")

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_users'])
            print(f"[Batch {batch_id}] Users Staging: {quarantine_df.count()} records quarantined")


def ingest_products_batch(batch_df, batch_id):
    """
    Validates product records and writes clean/quarantine Parquet files.
    """
    if batch_df.count() > 0:
        clean_df, quarantine_df = validate_products(batch_df)

        # Write clean data to staging
        clean_df.write.mode("append").parquet(PATHS['clean_products'])
        print(f"[Batch {batch_id}] Products Staging: {clean_df.count()} clean records written")

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_products'])
            print(f"[Batch {batch_id}] Products Staging: {quarantine_df.count()} records quarantined")


def ingest_views_batch(batch_df, batch_id):
    """
    Validates view records and writes clean/quarantine Parquet files.
    """
    if batch_df.count() > 0:
        clean_df, quarantine_df = validate_views(batch_df)

        # Write clean data to staging
        clean_df.write.mode("append").parquet(PATHS['clean_views'])
        print(f"[Batch {batch_id}] Views Staging: {clean_df.count()} clean records written")

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_views'])
            print(f"[Batch {batch_id}] Views Staging: {quarantine_df.count()} records quarantined")


def ingest_carts_batch(batch_df, batch_id):
    """
    Validates cart records and writes clean/quarantine Parquet files.
    """
    if batch_df.count() > 0:
        clean_df, quarantine_df = validate_carts(batch_df)

        # Write clean data to staging
        clean_df.write.mode("append").parquet(PATHS['clean_carts'])
        print(f"[Batch {batch_id}] Carts Staging: {clean_df.count()} clean records written")

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_carts'])
            print(f"[Batch {batch_id}] Carts Staging: {quarantine_df.count()} records quarantined")


def ingest_transactions_batch(batch_df, batch_id):
    """
    Validates transaction records and writes clean/quarantine Parquet files.
    """
    if batch_df.count() > 0:
        clean_df, quarantine_df = validate_transactions(batch_df)

        # Write clean data to staging
        clean_df.write.mode("append").parquet(PATHS['clean_transactions'])
        print(f"[Batch {batch_id}] Transactions Staging: {clean_df.count()} clean records written")

        # Write quarantined data
        if quarantine_df.count() > 0:
            quarantine_df.write.mode("append").parquet(PATHS['quarantine_transactions'])
            print(f"[Batch {batch_id}] Transactions Staging: {quarantine_df.count()} records quarantined")


# =============================================================================
# REAL-TIME METRICS AGGREGATION
# =============================================================================

def process_sales_metrics_batch(batch_df, batch_id):
    """
    Aggregate and publish sales metrics.
    Computes hourly, category, and country metrics and sends to Kafka.
    """
    if batch_df.count() > 0:
        spark = batch_df.sparkSession

        # Convert timestamp string to TimestampType for aggregation
        batch_with_ts = batch_df.withColumn("timestamp", to_timestamp(col("timestamp")))

        # --- Hourly Metrics ---
        # Each batch produces hourly aggregates for transactions in THAT batch
        hourly_metrics = aggregate_sales_by_hour(batch_with_ts)
        hourly_metrics = hourly_metrics \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("processed_at", current_timestamp())
        write_to_kafka(hourly_metrics, KAFKA_CONFIG['topics']['metrics_sales_hourly'])
        print(f"[Batch {batch_id}] Sales Metrics: Hourly metrics sent to Kafka")

        # --- Category Metrics ---
        try:
            product_df = spark.read.parquet(PATHS['clean_products'])
            category_metrics = aggregate_sales_by_category(batch_with_ts, product_df)
            category_metrics = category_metrics \
                .withColumn("batch_id", lit(batch_id)) \
                .withColumn("processed_at", current_timestamp())
            write_to_kafka(category_metrics, KAFKA_CONFIG['topics']['metrics_sales_category'])
            print(f"[Batch {batch_id}] Sales Metrics: Category metrics sent to Kafka")
        except Exception:
            print(f"[Batch {batch_id}] Sales Metrics: No product data available yet for category metrics")

        # --- Country/Region Metrics ---
        try:
            user_df = spark.read.parquet(PATHS['clean_users'])
            country_metrics = aggregate_sales_by_country(batch_with_ts, user_df)
            country_metrics = country_metrics \
                .withColumn("batch_id", lit(batch_id)) \
                .withColumn("processed_at", current_timestamp())
            write_to_kafka(country_metrics, KAFKA_CONFIG['topics']['metrics_sales_country'])
            print(f"[Batch {batch_id}] Sales Metrics: Country metrics sent to Kafka")
        except Exception:
            print(f"[Batch {batch_id}] Sales Metrics: No user data available yet for country metrics")


# =============================================================================
# ANOMALY DETECTION & ALERTS
# =============================================================================

def process_transaction_anomalies_batch(batch_df, batch_id):
    """
    Detect transaction anomalies and generate alerts.
    ONLY anomalous transactions go to metrics_anomalies topic.
    """
    if batch_df.count() > 0:
        # Detect anomalies (adds is_anomaly, transaction_anomaly, anomaly_detected_at columns)
        anomaly_df = detect_transaction_anomalies(batch_df)

        # Filter to ONLY actual anomalies
        # This is critical - writing all transactions floods the dashboard deque
        # with normal transactions, pushing out actual anomalies
        anomalies_only_df = anomaly_df.filter(col("is_anomaly") == True)

        anomaly_count = anomalies_only_df.count()
        if anomaly_count > 0:
            # Add batch_id for dashboard tracking
            anomalies_only_df = anomalies_only_df.withColumn("batch_id", lit(batch_id))

            # Generate alerts (sends to Kafka alerts topic + console/file)
            generate_transaction_alerts(anomalies_only_df)

            # Write ONLY anomalies to metrics topic for dashboard
            write_to_kafka(anomalies_only_df, KAFKA_CONFIG['topics']['metrics_anomalies'])

            print(f"[Batch {batch_id}] Transaction Anomalies: {anomaly_count} anomalies detected and sent to Kafka")
        else:
            print(f"[Batch {batch_id}] Transaction Anomalies: {batch_df.count()} transactions processed, no anomalies")


def process_inventory_alerts_batch(batch_df, batch_id):
    """
    Detect low inventory and generate alerts.
    Flags out-of-stock, critical, and low inventory products.
    """
    if batch_df.count() > 0:
        # Detect low inventory (adds inventory_status, needs_alert, alert_timestamp columns)
        inventory_df = detect_low_inventory(batch_df)

        # Filter to ONLY items that need alerts
        # This is critical - writing all products floods the dashboard deque with
        # normal-inventory items, pushing out actual alerts
        alerts_only_df = inventory_df.filter(col("needs_alert") == True)

        alert_count = alerts_only_df.count()
        if alert_count > 0:
            # Add batch_id for dashboard tracking
            alerts_only_df = alerts_only_df.withColumn("batch_id", lit(batch_id))

            # Generate alerts (sends to Kafka alerts topic + console/file)
            generate_inventory_alerts(alerts_only_df)

            # Write ONLY alerts to metrics topic for dashboard
            write_to_kafka(alerts_only_df, KAFKA_CONFIG['topics']['metrics_inventory_alerts'])

            print(f"[Batch {batch_id}] Inventory Alerts: {alert_count} low inventory alerts generated")
        else:
            print(f"[Batch {batch_id}] Inventory Status: {batch_df.count()} products processed, all normal")


def process_cart_abandonment_batch(batch_df, batch_id):
    """
    Detect abandoned carts by checking SAVED carts (from staging) against transactions.
    
    Only flags NEW abandoned carts - tracks already-processed carts to avoid duplicates.
    """
    from pyspark.sql.functions import expr
    
    spark = batch_df.sparkSession
    timeout_minutes = ANOMALY_DETECTION['cart_abandonment']['timeout_minutes']
    min_cart_value = ANOMALY_DETECTION['cart_abandonment']['min_cart_value']
    
    # Read ALL saved carts from staging (not just current batch)
    try:
        all_carts_df = spark.read.parquet(PATHS['clean_carts'])
    except Exception as e:
        print(f"[Batch {batch_id}] Cart Abandonment: No cart data in staging yet ({e})")
        return
    
    # Convert timestamp to proper type
    all_carts_df = all_carts_df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    # Calculate cart values
    cart_with_value = all_carts_df \
        .withColumn("product", explode("products")) \
        .withColumn("item_total", col("product.price") * col("product.quantity")) \
        .groupBy("cart_id", "user_id", "timestamp") \
        .agg(spark_sum("item_total").alias("cart_value"))
    
    # Filter to high-value carts
    high_value_carts = cart_with_value.filter(col("cart_value") >= min_cart_value)
    
    if high_value_carts.count() == 0:
        print(f"[Batch {batch_id}] Cart Abandonment: No high-value carts in staging")
        return
    
    # Filter carts that are OLD ENOUGH to be considered abandoned
    old_carts = high_value_carts.filter(
        col("timestamp") < expr(f"current_timestamp() - INTERVAL {timeout_minutes} MINUTES")
    )
    
    old_count = old_carts.count()
    if old_count == 0:
        print(f"[Batch {batch_id}] Cart Abandonment: No carts older than {timeout_minutes} min yet")
        return
    
    # Check against transactions to exclude carts where user completed purchase
    try:
        transactions_df = spark.read.parquet(PATHS['clean_transactions'])
        transactions_df = transactions_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        
        # left_anti join: keep carts where NO matching transaction exists
        abandoned_carts = old_carts.alias("cart").join(
            transactions_df.select("user_id", "timestamp").alias("trans"),
            (col("cart.user_id") == col("trans.user_id")) &
            (col("trans.timestamp") >= col("cart.timestamp")),
            how="left_anti"
        )
    except Exception as e:
        # No transaction data - all old carts are abandoned
        print(f"[Batch {batch_id}] Cart Abandonment: No transaction data, all old carts abandoned")
        abandoned_carts = old_carts
    
    # **FIX: Filter out carts already reported as abandoned**
    try:
        already_reported_df = spark.read.parquet(PATHS['clean_abandoned_carts'])
        # Only keep carts NOT in already_reported
        new_abandoned = abandoned_carts.join(
            already_reported_df.select("cart_id"),
            on="cart_id",
            how="left_anti"
        )
    except Exception:
        # First run - no previously reported carts
        print(f"[Batch {batch_id}] Cart Abandonment: First run, no tracking file yet")
        new_abandoned = abandoned_carts
    
    abandoned_count = new_abandoned.count()
    
    if abandoned_count > 0:
        # Add metadata
        result_df = new_abandoned \
            .withColumnRenamed("timestamp", "cart_timestamp") \
            .withColumn("abandonment_status", lit("abandoned")) \
            .withColumn("detected_at", current_timestamp()) \
            .withColumn("timeout_minutes", lit(timeout_minutes)) \
            .withColumn("batch_id", lit(batch_id))
        
        # **Write to Kafka for dashboard**
        write_to_kafka(result_df, KAFKA_CONFIG['topics']['metrics_abandoned_carts'])
        
        # **Save to tracking file to prevent re-reporting**
        result_df.write.mode("append").parquet(PATHS['clean_abandoned_carts'])
        
        print(f"[Batch {batch_id}] Cart Abandonment: {abandoned_count} NEW abandoned carts detected!")
    else:
        print(f"[Batch {batch_id}] Cart Abandonment: {old_count} old carts checked, all already reported or have transactions")



# =============================================================================
# MAIN STREAMING PIPELINE
# =============================================================================

def start_streaming_pipeline():
    """
    Main function to start all streaming pipelines.
    """
    print("\n" + "=" * 60)
    print("GlobalMart Stream Processing Pipeline")
    print("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # =========================================================================
    # READ STREAMS FROM KAFKA
    # =========================================================================

    # --- User Stream ---
    kafka_user_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['users']) \
        .option("kafka.group.id", "globalmart-user-streamer") \
        .option("startingOffsets", "latest") \
        .load()

    user_schema = define_user_schema()
    parsed_user_df = kafka_user_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), user_schema).alias("data")) \
        .select("data.*")

    # --- Product Stream ---
    kafka_product_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['products']) \
        .option("kafka.group.id", "globalmart-product-streamer") \
        .option("startingOffsets", "latest") \
        .load()

    product_schema = define_product_schema()
    parsed_product_df = kafka_product_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), product_schema).alias("data")) \
        .select("data.*")

    # --- Product View Stream ---
    kafka_view_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['views']) \
        .option("kafka.group.id", "globalmart-view-streamer") \
        .option("startingOffsets", "latest") \
        .load()

    view_schema = define_view_schema()
    parsed_view_df = kafka_view_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), view_schema).alias("data")) \
        .select("data.*")

    # --- Cart Stream ---
    kafka_cart_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['carts']) \
        .option("kafka.group.id", "globalmart-cart-streamer") \
        .option("startingOffsets", "latest") \
        .load()

    cart_schema = define_cart_schema()
    parsed_cart_df = kafka_cart_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), cart_schema).alias("data")) \
        .select("data.*")

    # --- Transaction Stream ---
    kafka_transaction_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['transactions']) \
        .option("kafka.group.id", "globalmart-transaction-streamer") \
        .option("startingOffsets", "latest") \
        .load()

    transaction_schema = define_transaction_schema()
    parsed_transaction_df = kafka_transaction_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), transaction_schema).alias("data")) \
        .select("data.*")

    # =========================================================================
    # DATA QUALITY / STAGING STREAMS
    # =========================================================================
    print("\nStarting Data Quality / Staging Streams...")

    user_staging_writer = parsed_user_df.writeStream \
        .outputMode("append") \
        .foreachBatch(ingest_users_batch) \
        .option("checkpointLocation", PATHS['checkpoint_users']) \
        .start()

    product_staging_writer = parsed_product_df.writeStream \
        .outputMode("append") \
        .foreachBatch(ingest_products_batch) \
        .option("checkpointLocation", PATHS['checkpoint_products']) \
        .start()

    view_staging_writer = parsed_view_df.writeStream \
        .outputMode("append") \
        .foreachBatch(ingest_views_batch) \
        .option("checkpointLocation", PATHS['checkpoint_views']) \
        .start()

    cart_staging_writer = parsed_cart_df.writeStream \
        .outputMode("append") \
        .foreachBatch(ingest_carts_batch) \
        .option("checkpointLocation", PATHS['checkpoint_carts']) \
        .start()

    transaction_staging_writer = parsed_transaction_df.writeStream \
        .outputMode("append") \
        .foreachBatch(ingest_transactions_batch) \
        .option("checkpointLocation", PATHS['checkpoint_transactions']) \
        .start()

    # =========================================================================
    # REAL-TIME METRICS STREAMS
    # =========================================================================
    print("Starting Real-Time Metrics Streams...")

    sales_metrics_writer = parsed_transaction_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_sales_metrics_batch) \
        .option("checkpointLocation", PATHS['checkpoint_sales_aggregation']) \
        .start()

    # =========================================================================
    # ANOMALY DETECTION & ALERT STREAMS
    # =========================================================================
    print("Starting Anomaly Detection & Alert Streams...")

    # Transaction Anomaly Detection (separate stream on transactions)
    transaction_anomaly_writer = parsed_transaction_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_transaction_anomalies_batch) \
        .option("checkpointLocation", PATHS['checkpoint_transaction_anomalies']) \
        .start()

    # Inventory Alert Detection (separate stream on products)
    inventory_alert_writer = parsed_product_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_inventory_alerts_batch) \
        .option("checkpointLocation", PATHS['checkpoint_inventory_alerts']) \
        .start()

    # Create a dedicated cart stream for abandonment detection
    kafka_cart_abandonment_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['carts']) \
        .option("kafka.group.id", "globalmart-cart-abandonment") \
        .option("startingOffsets", "latest") \
        .load()

    cart_for_abandonment = kafka_cart_abandonment_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), cart_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    cart_abandonment_writer = cart_for_abandonment.writeStream \
        .outputMode("append") \
        .foreachBatch(process_cart_abandonment_batch) \
        .option("checkpointLocation", PATHS['checkpoint_cart_abandonment']) \
        .start()

    # =========================================================================
    # SESSION ANALYSIS STREAM
    # =========================================================================
    print("Starting Session Analysis Stream...")

    # Create a dedicated view stream for session analysis
    # Triggered by new views but analyzes all stored activities
    kafka_view_sessions_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['views']) \
        .option("kafka.group.id", "globalmart-session-analyzer") \
        .option("startingOffsets", "latest") \
        .load()

    view_for_sessions = kafka_view_sessions_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), view_schema).alias("data")) \
        .select("data.*")

    session_analysis_writer = view_for_sessions.writeStream \
        .outputMode("append") \
        .foreachBatch(process_sessions_batch) \
        .option("checkpointLocation", PATHS['checkpoint_sessions']) \
        .start()

    print("\n" + "=" * 60)
    print("All Streaming Pipelines Started Successfully")
    print("\nSpark UI: http://localhost:4040")
    print("=" * 60 + "\n")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    try:
        # Clean old checkpoints if they exist (for fresh start during development)
        CHECKPOINT_DIR = PATHS['checkpoints']
        if os.path.exists(CHECKPOINT_DIR):
            print(f"Removing old checkpoint dir: {CHECKPOINT_DIR}")
            shutil.rmtree(CHECKPOINT_DIR)

        # Start the streaming pipeline
        start_streaming_pipeline()

    except KeyboardInterrupt:
        print("\n\nShutting down stream processing...")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
