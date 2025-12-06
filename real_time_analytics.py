"""
Real-Time Analytics Module
Performs anomaly detection, inventory tracking, and sales aggregation
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, window,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    explode, hour, date_format, countDistinct, stddev, expr
)
from config import ANOMALY_DETECTION, ALERT_CONFIG, KAFKA_CONFIG, PATHS
import json
import uuid
from datetime import datetime
from kafka import KafkaProducer


class RealTimeAnalytics:
    """
    Handles real-time analytics on clean streaming data
    """

    def __init__(self):
        self.anomaly_config = ANOMALY_DETECTION
        self.alert_config = ALERT_CONFIG
        self._kafka_producer = None # Initialize Kafka producer for alerts

    def get_kafka_producer(self):
        """Get or create Kafka producer for alerts"""
        if self._kafka_producer is None:
            try:
                self._kafka_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
            except Exception as e:
                print(f"Warning: Could not create Kafka producer for alerts: {e}")
        return self._kafka_producer

    # TRANSACTION ANOMALY DETECTION

    def detect_amount_based_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies based on transaction amount
        Flags transactions that are too high or too low
        """
        max_amount = self.anomaly_config['transaction']['max_amount']
        min_amount = self.anomaly_config['transaction']['min_amount']

        # Add anomaly flags
        df = df.withColumn(
            "transaction_anomaly",
            when(col("total_amount") > max_amount, lit("high"))
            .when(col("total_amount") < min_amount, lit("low"))
            .otherwise(lit("normal"))
        )

        df = df.withColumn(
            "is_anomaly",
            col("transaction_anomaly") != "normal"
        )

        # Add detection timestamp
        df = df.withColumn("anomaly_detected_at", current_timestamp())

        return df


    # INVENTORY STOCK DETECTION

    def detect_low_inventory(self, df: DataFrame) -> DataFrame:
        """
        Detect products with low inventory levels (either low, critical, or out of stock)
        """
        low_threshold = self.anomaly_config['inventory']['low_stock_threshold']
        critical_threshold = self.anomaly_config['inventory']['critical_stock_threshold']

        # Add inventory status flags
        df = df.withColumn(
            "inventory_status",
            when(col("inventory") == 0, lit("out_of_stock"))
            .when(col("inventory") <= critical_threshold, lit("critical"))
            .when(col("inventory") <= low_threshold, lit("low"))
            .otherwise(lit("normal"))
        )

        df = df.withColumn(
            "needs_alert",
            col("inventory_status").isin(["out_of_stock", "critical", "low"])
        )

        df = df.withColumn("alert_timestamp", current_timestamp())

        return df
    
        # ALERT GENERATION

    def generate_alert(self, alert_type: str, severity: str, message: str, data: dict = None):
        """
        Generate an alert and log it.
        Each alert gets a unique ID for tracking and deduplication.
        """
        if not self.alert_config['enabled']:
            return

        if alert_type not in self.alert_config['alert_types'] or \
           not self.alert_config['alert_types'][alert_type]:
            return

        alert = {
            "alert_id": str(uuid.uuid4()),  # Unique ID for each alert
            "timestamp": datetime.now().isoformat(),
            "type": alert_type,
            "severity": severity,
            "message": message,
            "data": data or {}
        }

        # Log to console if enabled
        if 'console' in self.alert_config['notification_methods']:
            print(f"\n🚨 ALERT [{severity}] - {alert_type}")
            print(f"   {message}")
            if data:
                print(f"   Details: {json.dumps(data, indent=2)}")

        # Send alert to Kafka stream
        try:
            producer = self.get_kafka_producer()
            if producer:
                producer.send(KAFKA_CONFIG['topics']['alerts'], value=alert)
                producer.flush()
        except Exception as e:
            print(f"Warning: Could not send alert to Kafka: {e}")

        # Log to file if enabled
        if 'file' in self.alert_config['notification_methods']:
            try:
                if 'alerts_log' in PATHS:
                    with open(PATHS['alerts_log'], 'a') as f:
                        f.write(json.dumps(alert) + '\n')
            except Exception as e:
                pass


    def process_transaction_alerts(self, df: DataFrame):
        """
        Process transaction anomalies and generate alerts
        """
        anomalies = df.filter(col("is_anomaly") == True)

        if anomalies.count() > 0:
            anomaly_list = anomalies.collect()
            for anomaly in anomaly_list:
                message = f"Transaction anomaly detected: {anomaly['transaction_id']}"
                data = {
                    "transaction_id": anomaly['transaction_id'],
                    "user_id": anomaly['user_id'],
                    "amount": float(anomaly['total_amount']),
                    "transaction_anomaly": anomaly['transaction_anomaly']
                }
                self.generate_alert(
                    alert_type="transaction_anomaly",
                    severity="high",
                    message=message,
                    data=data
                )

    def process_inventory_alerts(self, df: DataFrame):
        """
        Process inventory alerts and generate notifications
        """
        alerts = df.filter(col("needs_alert") == True)

        if alerts.count() > 0:
            alert_list = alerts.collect()
            for alert in alert_list:
                status = alert['inventory_status']
                severity = "critical" if status in ["out_of_stock", "critical"] else "medium"

                message = f"{status.upper()}: Product {alert['product_id']} has {alert['inventory']} units left"

                # Safely get category field - handle Row objects and missing fields
                try:
                    category = alert['category'] if alert['category'] is not None else 'unknown'
                except (KeyError, ValueError):
                    category = 'unknown'

                data = {
                    "product_id": alert['product_id'],
                    "inventory": int(alert['inventory']),
                    "status": status,
                    "category": category
                }

                self.generate_alert(
                    alert_type="low_inventory",
                    severity=severity,
                    message=message,
                    data=data
                )
            
    # SALES AGGREGATION

    def aggregate_sales_by_hour(self, df: DataFrame) -> DataFrame:
        """
        Aggregate comprehensive sales metrics by hour
        Return multiple metrics including revenue, customer, and statistical measures
        """

        hourly_sales = df.withColumn("hour", hour(col("timestamp"))) \
            .groupBy("hour") \
            .agg(
                # Revenue metrics
                spark_sum("total_amount").alias("total_sales"),
                count("transaction_id").alias("transaction_count"),
                avg("total_amount").alias("avg_transaction_value"),

                # Statistical metrics
                spark_min("total_amount").alias("min_transaction_value"),
                spark_max("total_amount").alias("max_transaction_value"),
                stddev("total_amount").alias("stddev_transaction_value"),

                # Customer metrics
                countDistinct("user_id").alias("unique_customers"),
                (count("transaction_id") / countDistinct("user_id")).alias("avg_transactions_per_customer"),

            ) \
            .withColumn("sales_velocity", col("total_sales") / lit(3600))  # Sales per second in that hour

        return hourly_sales


    # AGGREGATION BY CATEGORY
    def aggregate_sales_by_category(self, df: DataFrame, product_df: DataFrame = None) -> DataFrame:
        """
        Aggregate comprehensive sales metrics by product category
        """
        # Explode products array to get individual products
        exploded_df = df.select(
            "transaction_id",
            "user_id",
            "timestamp",
            explode("products").alias("product")
        )

        # Extract product details and calculate revenue
        product_level_df = exploded_df.select(
            "transaction_id",
            "user_id",
            "timestamp",
            col("product.product_id").alias("product_id"),
            col("product.quantity").alias("quantity"),
            col("product.price").alias("price")
        ).withColumn(
            "product_revenue",
            col("quantity") * col("price")
        )

        # Aggregate by product_id
        product_metrics = product_level_df.groupBy("product_id") \
            .agg(
                # Revenue metrics
                spark_sum("product_revenue").alias("total_revenue"),
                spark_sum("quantity").alias("total_units_sold"),
                count("transaction_id").alias("transaction_count"),

                # Statistical metrics
                avg("product_revenue").alias("avg_revenue_per_transaction"),
                avg("quantity").alias("avg_quantity_per_transaction"),
                avg("price").alias("avg_price"),

                # Customer metrics
                countDistinct("user_id").alias("unique_customers")
            )

        # If product catalog is provided, join to get category and aggregate
        if product_df is not None:
            # Join with product catalog to get category
            category_df = product_metrics.join(
                product_df.select("product_id", "category"),
                on="product_id",
                how="left"
            )

            # Aggregate by category
            category_metrics = category_df.groupBy("category") \
                .agg(
                    spark_sum("total_revenue").alias("total_revenue"),
                    spark_sum("total_units_sold").alias("total_units_sold"),
                    spark_sum("transaction_count").alias("transaction_count"),
                    countDistinct("product_id").alias("unique_products"),
                    avg("avg_price").alias("avg_product_price"),
                    countDistinct("unique_customers").alias("total_unique_customers")
                )

            return category_metrics

        return product_metrics

    def aggregate_sales_by_country(self, df: DataFrame, user_df: DataFrame = None) -> DataFrame:
        """
        Aggregate comprehensive sales metrics by country (region)
        """
        
        # If user DataFrame is provided, join to get country
        if user_df is not None:
            # Join transactions with users to get country
            trans_with_country = df.join(
                user_df.select("user_id", "country"),
                on="user_id",
                how="left"
            )

            # Aggregate by country
            country_metrics = trans_with_country.groupBy("country") \
                .agg(
                    # Revenue metrics
                    spark_sum("total_amount").alias("total_sales"),
                    count("transaction_id").alias("transaction_count"),
                    avg("total_amount").alias("avg_transaction_value"),

                    # Statistical metrics
                    spark_min("total_amount").alias("min_transaction_value"),
                    spark_max("total_amount").alias("max_transaction_value"),
                    stddev("total_amount").alias("stddev_transaction_value"),

                    # Customer metrics
                    countDistinct("user_id").alias("unique_customers"),
                    (count("transaction_id") / countDistinct("user_id")).alias("avg_transactions_per_customer")
                )

            # Add geographic distribution percentage
            total_sales = country_metrics.agg(spark_sum("total_sales").alias("total")).collect()[0]["total"]
            if total_sales:
                country_metrics = country_metrics.withColumn(
                    "sales_percentage",
                    (col("total_sales") / lit(total_sales)) * 100
                )

            return country_metrics

        return df                

# Abstracted Functions for easier integration
# Anomaly Detection 
def detect_transaction_anomalies(df: DataFrame) -> DataFrame:
    analyzer = RealTimeAnalytics()
    return analyzer.detect_amount_based_anomalies(df)

def detect_low_inventory(df: DataFrame) -> DataFrame:
    analyzer = RealTimeAnalytics()
    return analyzer.detect_low_inventory(df)

def generate_transaction_alerts(df: DataFrame):
    analyzer = RealTimeAnalytics()
    analyzer.process_transaction_alerts(df)

def generate_inventory_alerts(df: DataFrame):
    analyzer = RealTimeAnalytics()
    analyzer.process_inventory_alerts(df)

# Sales Aggregation
def aggregate_sales_by_hour(df: DataFrame) -> DataFrame:
    analyzer = RealTimeAnalytics()
    return analyzer.aggregate_sales_by_hour(df)

def aggregate_sales_by_category(df: DataFrame, product_df: DataFrame = None) -> DataFrame:
    analyzer = RealTimeAnalytics()
    return analyzer.aggregate_sales_by_category(df, product_df)

def aggregate_sales_by_country(df: DataFrame, user_df: DataFrame = None) -> DataFrame:
    analyzer = RealTimeAnalytics()
    return analyzer.aggregate_sales_by_country(df, user_df)


