"""
Real-Time Analytics Streaming Pipeline (Lambda Architecture - Speed Layer)
Reads from Kafka, performs real-time analytics, and writes metrics back to Kafka.
This pipeline ONLY handles real-time analytics - no disk persistence.
"""
import os
import sys
from datetime import datetime

# Set SPARK_HOME to use PySpark from virtual environment
pyspark_path = os.path.join(sys.prefix, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages', 'pyspark')
os.environ['SPARK_HOME'] = pyspark_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct, lit, expr,
    sum as spark_sum, count, avg, max as spark_max, 
    min as spark_min, stddev, approx_count_distinct, hour, explode, current_timestamp,
    to_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, ArrayType, IntegerType
)
import shutil

from config import KAFKA_CONFIG, PATHS, SPARK_CONFIG, ANOMALY_DETECTION


# ============== Schema Definitions ==============
# Reuse schemas from stream_processing.py

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


# ============== Spark Session ==============

def create_spark_session():
    """Create Spark session for real-time analytics"""
    return SparkSession.builder \
        .appName(f"{SPARK_CONFIG['app_name']} - Real-Time Analytics") \
        .config("spark.jars.packages", SPARK_CONFIG['packages']) \
        .config("spark.sql.shuffle.partitions", SPARK_CONFIG['shuffle_partitions']) \
        .getOrCreate()


# ============== Helper Function to Write to Kafka ==============

def write_to_kafka_topic(df, topic_name, checkpoint_path):
    """
    Write DataFrame to Kafka topic as JSON
    Uses writeStream.format("kafka") directly - no foreachBatch
    """
    return df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", topic_name) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()


# ============== Real-Time Analytics Streaming Queries ==============

def start_transaction_anomaly_detection(spark):
    """
    Detect anomalies in transaction events.
    anomlies can be based on amount thresholds and velocity (too many transactions in short time).
    """
    print("🔍 Starting Transaction Anomaly Detection...")
    
    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['transactions']) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "rt-analytics-anomalies") \
        .load()
    
    trans_schema = define_transaction_schema()
    
    parsed_trans = trans_df \
        .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), trans_schema).alias("data"), col("kafka_ts")) \
        .select("data.*", "kafka_ts")
    
    # Add event_time column with watermark for velocity detection
    parsed_trans = parsed_trans.withColumn(
        "event_time",
        when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
        .otherwise(col("kafka_ts"))
    ).withWatermark("event_time", "10 minutes")
    
    # Get thresholds from config
    max_amount = ANOMALY_DETECTION['transaction']['max_amount']
    min_amount = ANOMALY_DETECTION['transaction']['min_amount']
    max_trans_per_min = ANOMALY_DETECTION['transaction']['max_transactions_per_minute']
    velocity_window = ANOMALY_DETECTION['transaction']['velocity_window_minutes']
    
    # Define batch processing function for velocity detection
    def process_anomaly_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        # Step 1: Detect amount-based anomalies
        amount_anomalies = batch_df.withColumn(
            "anomaly_type",
            when(col("total_amount") > max_amount, lit("high_amount"))
            .when(col("total_amount") < min_amount, lit("low_amount"))
            .otherwise(lit("normal"))
        )
        
        # Step 2: Detect velocity-based anomalies
        # Count transactions per user in the batch window
        from pyspark.sql.window import Window
        from pyspark.sql.functions import window as time_window, row_number
        
        # Add a time window for grouping
        windowed_df = amount_anomalies.withColumn(
            "time_bucket",
            expr(f"window(event_time, '{velocity_window} minutes')")
        )
        
        # Count transactions per user per time bucket
        user_velocity = windowed_df.groupBy("user_id", "time_bucket") \
            .agg(count("transaction_id").alias("trans_count"))
        
        # Join back to get transaction counts
        with_velocity = amount_anomalies.alias("trans").join(
            user_velocity.alias("vel"),
            (col("trans.user_id") == col("vel.user_id")) &
            (expr(f"trans.event_time >= vel.time_bucket.start") &
             expr(f"trans.event_time < vel.time_bucket.end")),
            "left"
        ).select("trans.*", col("vel.trans_count"))
        
        # Update anomaly type if velocity is high
        final_anomalies = with_velocity.withColumn(
            "anomaly_type",
            when((col("trans_count") > max_trans_per_min) & (col("anomaly_type") == "normal"), 
                 lit("high_velocity"))
            .when((col("trans_count") > max_trans_per_min) & (col("anomaly_type") != "normal"),
                 expr("concat(anomaly_type, '_high_velocity')"))
            .otherwise(col("anomaly_type"))
        )
        
        # Filter only actual anomalies
        anomalies_only = final_anomalies.filter(col("anomaly_type") != "normal")
        
        if anomalies_only.isEmpty():
            return
        
        # Prepare output
        output_df = anomalies_only.select(
            col("transaction_id"),
            col("user_id"),
            col("total_amount"),
            col("payment_method"),
            col("anomaly_type"),
            col("event_time").cast("string").alias("detected_at")
        )
        
        # Write to Kafka
        output_df.selectExpr(
            "transaction_id as key",
            "to_json(struct(*)) as value"
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
            .option("topic", KAFKA_CONFIG['topics']['metrics_anomalies']) \
            .save()
    
    # Use foreachBatch for velocity detection
    query = parsed_trans \
        .writeStream \
        .foreachBatch(process_anomaly_batch) \
        .option("checkpointLocation", PATHS['checkpoint_rt_anomalies']) \
        .start()
    
    print("✓ Transaction Anomaly Detection query started (amount + velocity)")
    return query


def start_inventory_alert_detection(spark):
    """
    Monitor product inventory levels and generate alerts for low stock.
    """
    print("🔍 Starting Inventory Alert Detection...")
    
    product_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['products']) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "rt-analytics-inventory") \
        .load()
    
    product_schema = define_product_schema()
    
    parsed_products = product_df \
        .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), product_schema).alias("data"), col("kafka_ts")) \
        .select("data.*", "kafka_ts")
    
    # Get thresholds from config
    critical_threshold = ANOMALY_DETECTION['inventory']['critical_stock_threshold']
    low_threshold = ANOMALY_DETECTION['inventory']['low_stock_threshold']
    
    # Detect low inventory
    alerts = parsed_products.withColumn(
        "alert_type",
        when(col("inventory") == 0, lit("out_of_stock"))
        .when(col("inventory") <= critical_threshold, lit("critical_stock"))
        .when(col("inventory") <= low_threshold, lit("low_stock"))
        .otherwise(lit("normal"))
    )
    
    # Filter only actual alerts
    alerts_only = alerts.filter(col("alert_type") != "normal")
    
    # Prepare output
    output_df = alerts_only.select(
        col("product_id"),
        col("category"),
        col("inventory"),
        col("price"),
        col("alert_type"),
        col("kafka_ts").cast("string").alias("detected_at")
    )
    
    # Convert to JSON for Kafka
    kafka_output = output_df.select(
        col("product_id").alias("key"),
        to_json(struct(
            col("product_id"),
            col("category"),
            col("inventory"),
            col("price"),
            col("alert_type"),
            col("detected_at")
        )).alias("value")
    )
    
    query = kafka_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", KAFKA_CONFIG['topics']['metrics_inventory_alerts']) \
        .option("checkpointLocation", PATHS['checkpoint_rt_inventory_alerts']) \
        .outputMode("append") \
        .start()
    
    print("✓ Inventory Alert Detection query started")
    return query


def start_cart_abandonment_detection(spark):
    """
    Detect abandoned shopping carts.
    Uses stream-to-stream left outer join to check if carts were converted to transactions.
    Only flags carts as abandoned if no transaction occurs within timeout period.
    """
    print("🔍 Starting Cart Abandonment Detection...")
    
    timeout_minutes = ANOMALY_DETECTION['cart_abandonment']['timeout_minutes']
    min_cart_value = ANOMALY_DETECTION['cart_abandonment']['min_cart_value']
    
    # Read cart stream
    cart_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['carts']) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "rt-analytics-carts") \
        .load()
    
    cart_schema = define_cart_schema()
    
    parsed_carts = cart_df \
        .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), cart_schema).alias("data"), col("kafka_ts")) \
        .select("data.*", "kafka_ts")
    
    # Add event_time with watermark
    carts_with_time = parsed_carts.withColumn(
        "cart_time",
        when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
        .otherwise(col("kafka_ts"))
    ).withWatermark("cart_time", f"{timeout_minutes} minutes")
    
    # Calculate cart value
    carts_with_value = carts_with_time.withColumn(
        "cart_value",
        expr("aggregate(products, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x.price * CAST(x.quantity AS DOUBLE))")
    )
    
    # Filter high-value carts only
    high_value_carts = carts_with_value.filter(col("cart_value") >= min_cart_value)
    
    # Read transaction stream
    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['transactions']) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "rt-analytics-carts-trans") \
        .load()
    
    trans_schema = define_transaction_schema()
    
    parsed_trans = trans_df \
        .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), trans_schema).alias("data"), col("kafka_ts")) \
        .select("data.*", "kafka_ts")
    
    # Add transaction time with watermark
    trans_with_time = parsed_trans.withColumn(
        "trans_time",
        when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
        .otherwise(col("kafka_ts"))
    ).withWatermark("trans_time", f"{timeout_minutes} minutes")
    
    # Left outer join: cart with transaction within timeout window
    # Join condition: same user AND transaction within timeout period
    joined = high_value_carts.alias("cart").join(
        trans_with_time.select("user_id", "transaction_id", "trans_time").alias("trans"),
        expr(f"""
            cart.user_id = trans.user_id AND
            trans.trans_time >= cart.cart_time AND
            trans.trans_time <= cart.cart_time + INTERVAL {timeout_minutes} MINUTES
        """),
        "leftOuter"
    )
    
    # Carts with no matching transaction are abandoned
    abandoned_carts = joined.filter(col("trans.transaction_id").isNull()) \
        .select(
            col("cart.cart_id").alias("cart_id"),
            col("cart.user_id").alias("user_id"),
            col("cart.cart_value").alias("cart_value"),
            col("cart.products").alias("products"),
            col("cart.cart_time").cast("string").alias("cart_created_at"),
            lit("abandoned").alias("status"),
            current_timestamp().cast("string").alias("detected_at")
        )
    
    # Convert to JSON for Kafka
    kafka_output = abandoned_carts.select(
        col("cart_id").alias("key"),
        to_json(struct(
            col("cart_id"),
            col("user_id"),
            col("cart_value"),
            col("products"),
            col("cart_created_at"),
            col("status"),
            col("detected_at")
        )).alias("value")
    )
    
    query = kafka_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", KAFKA_CONFIG['topics']['metrics_abandoned_carts']) \
        .option("checkpointLocation", PATHS['checkpoint_rt_abandoned_carts']) \
        .outputMode("append") \
        .start()
    
    print("✓ Cart Abandonment Detection query started")
    return query


def start_sales_hourly_aggregation(spark):
    """
    Aggregate sales metrics by hour.
    """
    print("📊 Starting Hourly Sales Aggregation...")
    
    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['transactions']) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "rt-analytics-sales-hourly") \
        .load()
    
    trans_schema = define_transaction_schema()
    
    parsed_trans = trans_df \
        .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), trans_schema).alias("data"), col("kafka_ts")) \
        .select("data.*", "kafka_ts")
    
    # Add event_time with watermark
    parsed_trans = parsed_trans.withColumn(
        "event_time",
        when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
        .otherwise(col("kafka_ts"))
    ).withWatermark("event_time", "20 minutes")
    
    # Aggregate by hour
    hourly_sales = parsed_trans \
        .withColumn("hour", hour(col("event_time"))) \
        .groupBy("hour") \
        .agg(
            spark_sum("total_amount").alias("total_sales"),
            count("transaction_id").alias("transaction_count"),
            avg("total_amount").alias("avg_transaction_value"),
            approx_count_distinct("user_id").alias("unique_customers")
        )
    
    # Prepare output
    output_df = hourly_sales.select(
        col("hour"),
        col("total_sales"),
        col("transaction_count"),
        col("avg_transaction_value"),
        col("unique_customers"),
        current_timestamp().cast("string").alias("computed_at")
    )
    
    # Convert to JSON for Kafka
    kafka_output = output_df.select(
        col("hour").cast("string").alias("key"),
        to_json(struct(
            col("hour"),
            col("total_sales"),
            col("transaction_count"),
            col("avg_transaction_value"),
            col("unique_customers"),
            col("computed_at")
        )).alias("value")
    )
    
    query = kafka_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", KAFKA_CONFIG['topics']['metrics_sales_hourly']) \
        .option("checkpointLocation", PATHS['checkpoint_rt_sales_hourly']) \
        .outputMode("update") \
        .start()
    
    print("✓ Hourly Sales Aggregation query started")
    return query


def start_sales_category_aggregation(spark):
    """
    Aggregate sales metrics by product category.
    Uses foreachBatch to join streaming transactions with static product data.
    """
    print("📊 Starting Sales by Category Aggregation...")
    
    # Read transaction stream
    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['transactions']) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "rt-analytics-sales-category") \
        .load()
    
    trans_schema = define_transaction_schema()
    
    parsed_trans = trans_df \
        .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), trans_schema).alias("data"), col("kafka_ts")) \
        .select("data.*", "kafka_ts")
    
    # Explode products array
    exploded_trans = parsed_trans \
        .select("transaction_id", "user_id", explode("products").alias("product")) \
        .select(
            "transaction_id",
            "user_id",
            col("product.product_id").alias("product_id"),
            col("product.quantity").alias("quantity"),
            col("product.price").alias("price")
        ) \
        .withColumn("product_revenue", col("quantity").cast("double") * col("price"))
    
    # Define batch processing function
    def process_category_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        # Read latest product data from disk (batch layer)
        try:
            product_static = spark.read.parquet(PATHS['clean_products']) \
                .select("product_id", "category")
        except:
            print("Warning: Could not read product data from disk")
            return
        
        # Join with static product data
        enriched = batch_df.join(product_static, "product_id", "inner")
        
        # Aggregate by category
        category_sales = enriched.groupBy("category") \
            .agg(
                spark_sum("product_revenue").alias("total_revenue"),
                spark_sum("quantity").alias("total_units_sold"),
                count("transaction_id").alias("transaction_count"),
                approx_count_distinct("product_id").alias("unique_products"),
                approx_count_distinct("user_id").alias("total_unique_customers"),
                avg("price").alias("avg_product_price")
            )
        
        # Add timestamp and send to Kafka
        output_df = category_sales.withColumn("computed_at", current_timestamp().cast("string"))
        
        # Write to Kafka
        output_df.selectExpr(
            "category as key",
            "to_json(struct(*)) as value"
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
            .option("topic", KAFKA_CONFIG['topics']['metrics_sales_category']) \
            .save()
    
    # Use foreachBatch to process
    query = exploded_trans \
        .writeStream \
        .foreachBatch(process_category_batch) \
        .option("checkpointLocation", PATHS['checkpoint_rt_sales_category']) \
        .start()
    
    print("✓ Sales by Category Aggregation query started")
    return query


def start_sales_country_aggregation(spark):
    """
    Aggregate sales metrics by user country.
    Uses foreachBatch to join streaming transactions with static user data.
    """
    print("📊 Starting Sales by Country Aggregation...")
    
    # Read transaction stream
    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topics']['transactions']) \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "rt-analytics-sales-country") \
        .load()
    
    trans_schema = define_transaction_schema()
    
    parsed_trans = trans_df \
        .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), trans_schema).alias("data"), col("kafka_ts")) \
        .select("data.*", "kafka_ts")
    
    # Define batch processing function
    def process_country_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        # Read latest user data from disk (batch layer)
        try:
            user_static = spark.read.parquet(PATHS['clean_users']) \
                .select("user_id", "country")
        except:
            print("Warning: Could not read user data from disk")
            return
        
        # Join with static user data
        enriched = batch_df.join(user_static, "user_id", "inner")
        
        # Aggregate by country
        country_sales = enriched.groupBy("country") \
            .agg(
                spark_sum("total_amount").alias("total_sales"),
                count("transaction_id").alias("transaction_count"),
                avg("total_amount").alias("avg_transaction_value"),
                approx_count_distinct("user_id").alias("unique_customers")
            )
        
        # Add timestamp and send to Kafka
        output_df = country_sales.withColumn("computed_at", current_timestamp().cast("string"))
        
        # Write to Kafka
        output_df.selectExpr(
            "country as key",
            "to_json(struct(*)) as value"
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
            .option("topic", KAFKA_CONFIG['topics']['metrics_sales_country']) \
            .save()
    
    # Use foreachBatch to process
    query = parsed_trans \
        .writeStream \
        .foreachBatch(process_country_batch) \
        .option("checkpointLocation", PATHS['checkpoint_rt_sales_country']) \
        .start()
    
    print("✓ Sales by Country Aggregation query started")
    return query


# ============== Main Pipeline ==============

def start_real_time_analytics_pipeline():
    """
    Start the real-time analytics pipeline.
    Multiple independent streaming queries for different metrics.
    """
    print("\n" + "="*60)
    print("Starting Real-Time Analytics Pipeline (Lambda Speed Layer)")
    print("="*60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Start all real-time analytics streams
    queries = []
    
    # 1. Transaction Anomaly Detection
    queries.append(start_transaction_anomaly_detection(spark))
    
    # 2. Inventory Alert Detection
    queries.append(start_inventory_alert_detection(spark))
    
    # 3. Cart Abandonment Detection
    queries.append(start_cart_abandonment_detection(spark))
    
    # 4. Sales Hourly Aggregation
    queries.append(start_sales_hourly_aggregation(spark))
    
    # 5. Sales Category Aggregation
    queries.append(start_sales_category_aggregation(spark))
    
    # 6. Sales Country Aggregation
    queries.append(start_sales_country_aggregation(spark))
    
    print("\n✓ Real-Time Analytics Pipeline Started")
    print(f"  - Active Streams: {len(queries)}")
    print("  - Metrics Output: Kafka topics (metrics_*)")
    print("  - Spark UI: http://localhost:4040")
    print("\n  Note: Batch persistence runs separately via stream_processing.py")
    print("\n" + "="*60)
    print("Monitoring Streaming Queries...")
    print("="*60)
    
    # Monitor streaming progress
    import time
    
    query_names = [
        "Transaction Anomaly Detection",
        "Inventory Alert Detection", 
        "Cart Abandonment Detection",
        "Sales Hourly Aggregation",
        "Sales Category Aggregation",
        "Sales Country Aggregation"
    ]
    
    try:
        iteration = 0
        while True:
            iteration += 1
            time.sleep(5)  # Update every 5 seconds
            
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Status Check #{iteration}")
            print("-" * 60)
            
            active_count = 0
            processing_count = 0
            
            for i, query in enumerate(queries):
                try:
                    status = query.status
                    progress = query.lastProgress
                    
                    is_active = status.get('isDataAvailable', False)
                    message = status.get('message', 'Unknown')
                    
                    if is_active:
                        active_count += 1
                    
                    status_icon = "🟢" if is_active else "🟡"
                    
                    if progress:
                        num_input = progress.get('numInputRows', 0)
                        if num_input > 0:
                            processing_count += 1
                            input_rate = progress.get('inputRowsPerSecond', 0)
                            print(f"{status_icon} [{i+1}] {query_names[i]}: {num_input} rows, {input_rate:.1f} rows/sec")
                        else:
                            print(f"{status_icon} [{i+1}] {query_names[i]}: Waiting for data...")
                    else:
                        print(f"{status_icon} [{i+1}] {query_names[i]}: {message}")
                        
                except Exception as e:
                    print(f"❌ [{i+1}] {query_names[i]}: Error - {str(e)}")
            
            print(f"\nSummary: {active_count}/6 active, {processing_count}/6 processing data")
            
    except KeyboardInterrupt:
        print("\n\n⚠ Stopping monitoring...")
        raise


if __name__ == "__main__":
    try:
        # Clean old real-time checkpoints if they exist
        RT_CHECKPOINTS = [
            PATHS['checkpoint_rt_anomalies'],
            PATHS['checkpoint_rt_inventory_alerts'],
            PATHS['checkpoint_rt_abandoned_carts'],
            PATHS['checkpoint_rt_sales_hourly'],
            PATHS['checkpoint_rt_sales_category'],
            PATHS['checkpoint_rt_sales_country']
        ]
        
        for checkpoint_path in RT_CHECKPOINTS:
            if os.path.exists(checkpoint_path):
                print(f"Removing old checkpoint: {checkpoint_path}")
                shutil.rmtree(checkpoint_path)
        
        start_real_time_analytics_pipeline()
        
    except KeyboardInterrupt:
        print("\n\n⚠ Shutting down real-time analytics pipeline...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
