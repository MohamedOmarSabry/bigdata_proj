"""
Real-Time Analytics Module
Performs anomaly detection, inventory tracking, cart abandonment, and sales aggregation
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, window,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    explode, hour, date_format, countDistinct, stddev, expr
)
from config import ANOMALY_DETECTION, ALERT_CONFIG, PATHS
import json
from datetime import datetime


class RealTimeAnalytics:
    """
    Handles real-time analytics on clean streaming data
    """

    def __init__(self):
        self.anomaly_config = ANOMALY_DETECTION
        self.alert_config = ALERT_CONFIG

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
        Generate an alert and log it
        """
        if not self.alert_config['enabled']:
            return

        if alert_type not in self.alert_config['alert_types'] or \
           not self.alert_config['alert_types'][alert_type]:
            return

        alert = {
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

        # Log to file if enabled
        if 'file' in self.alert_config['notification_methods']:
            with open(PATHS['alerts_log'], 'a') as f:
                f.write(json.dumps(alert) + '\n')


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
                data = {
                    "product_id": alert['product_id'],
                    "inventory": int(alert['inventory']),
                    "status": status,
                    "category": alert.get('category', 'unknown')
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

    def process_cart_abandonment_alerts(self, abandoned_carts_df: DataFrame):
        """
        Generate alerts for abandoned high-value carts
        """
        if abandoned_carts_df.count() > 0:
            abandoned_list = abandoned_carts_df.collect()

            for cart in abandoned_list:
                message = f"High-value cart abandoned: {cart['cart_id']} (${cart['cart_value']:.2f})"
                data = {
                    "cart_id": cart['cart_id'],
                    "user_id": cart['user_id'],
                    "cart_value": float(cart['cart_value']),
                    "cart_timestamp": str(cart['timestamp'])
                }

                self.generate_alert(
                    alert_type="cart_abandonment",
                    severity="medium",
                    message=message,
                    data=data
                )
                
        # CART ABANDONMENT DETECTION

    def detect_cart_abandonment(self, cart_stream_df: DataFrame, transaction_stream_df: DataFrame) -> DataFrame:
        """
        Detect abandoned carts using watermarking and stream-stream joins

        This properly tracks carts over time and only flags them as abandoned
        if no transaction occurs within the configured timeout period.
.
        """
        
        timeout_minutes = self.anomaly_config['cart_abandonment']['timeout_minutes']
        min_cart_value = self.anomaly_config['cart_abandonment']['min_cart_value']

        # First we add watermarking to both streams (5 mins)
        cart_with_watermark = cart_stream_df.withWatermark("timestamp", "5 minutes")
        transaction_with_watermark = transaction_stream_df.withWatermark("timestamp", "5 minutes")

        # Find how much each card is worth by aggregating product prices
        cart_with_value = cart_with_watermark.withColumn("product", explode("products")) \
            .withColumn("item_total", col("product.price") * col("product.quantity")) \
            .groupBy("cart_id", "user_id", "timestamp") \
            .agg(spark_sum("item_total").alias("cart_value"))

        # Only consider high-value carts which are greater than a configured threshold
        high_value_carts = cart_with_value.filter(col("cart_value") >= min_cart_value)

        # Now we want to match the carts with transactions made within the timeout period
        # join logic: same user, transaction made after carrt time and before the timeout period
        
        joined = high_value_carts.alias("cart").join(
            transaction_with_watermark.select("user_id", "timestamp", "transaction_id").alias("trans"),
            expr(f"""
                cart.user_id = trans.user_id AND
                trans.timestamp >= cart.timestamp AND
                trans.timestamp <= cart.timestamp + INTERVAL {timeout_minutes} MINUTES
            """),
            how="left_outer"
        )

        # Carts with no matching transaction are considered abandoned
        abandoned_carts = joined.filter(col("trans.transaction_id").isNull()) \
            .select(
                col("cart.cart_id").alias("cart_id"),
                col("cart.user_id").alias("user_id"),
                col("cart.timestamp").alias("cart_timestamp"),
                col("cart.cart_value").alias("cart_value")
            )

        # Add abandonment metadata about the status of the cart, timestamp and the timeout interval used
        abandoned_carts = abandoned_carts.withColumn(
            "abandonment_status",
            lit("abandoned")
        )
        abandoned_carts = abandoned_carts.withColumn(
            "detected_at",
            current_timestamp()
        )
        abandoned_carts = abandoned_carts.withColumn(
            "timeout_minutes",
            lit(timeout_minutes)
        )

        return abandoned_carts

# Abstracted Functions for easier integration
def detect_transaction_anomalies(df: DataFrame) -> DataFrame:
    """Detect amount-based transaction anomalies"""
    analyzer = RealTimeAnalytics()
    return analyzer.detect_amount_based_anomalies(df)

def detect_low_inventory(df: DataFrame) -> DataFrame:
    """Detect low inventory products"""
    analyzer = RealTimeAnalytics()
    return analyzer.detect_low_inventory(df)

def generate_transaction_alerts(df: DataFrame):
    """Generate alerts for transaction anomalies"""
    analyzer = RealTimeAnalytics()
    analyzer.process_transaction_alerts(df)

def generate_inventory_alerts(df: DataFrame):
    """Generate alerts for low inventory"""
    analyzer = RealTimeAnalytics()
    analyzer.process_inventory_alerts(df)
    
def detect_cart_abandonment(cart_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Detect abandoned carts using watermarking and time-based matching

    Args:
        cart_df: DataFrame of cart events (must have timestamp as TimestampType)
        transaction_df: DataFrame of transaction events (must have timestamp as TimestampType)

    Returns:
        DataFrame of abandoned carts
    """
    analyzer = RealTimeAnalytics()
    return analyzer.detect_cart_abandonment(cart_df, transaction_df)

def aggregate_sales_by_hour(df: DataFrame) -> DataFrame:
    """Aggregate sales metrics by hour"""
    analyzer = RealTimeAnalytics()
    return analyzer.aggregate_sales_by_hour(df)

def aggregate_sales_by_category(df: DataFrame, product_df: DataFrame = None) -> DataFrame:
    """Aggregate sales by category (optionally provide product_df for category-level aggregation)"""
    analyzer = RealTimeAnalytics()
    return analyzer.aggregate_sales_by_category(df, product_df)

def aggregate_sales_by_country(df: DataFrame, user_df: DataFrame = None) -> DataFrame:
    """Aggregate sales by country (optionally provide user_df for country-level aggregation)"""
    analyzer = RealTimeAnalytics()
    return analyzer.aggregate_sales_by_country(df, user_df)

def generate_cart_abandonment_alerts(df: DataFrame):
    """Generate alerts for abandoned carts"""
    analyzer = RealTimeAnalytics()
    analyzer.process_cart_abandonment_alerts(df)


