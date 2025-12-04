"""
Real-Time Analytics Module
Performs anomaly detection, inventory tracking, cart abandonment, and sales aggregation
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, window,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    explode, hour, date_format
)
from config import ANOMALY_DETECTION, ALERT_CONFIG
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
            from config import PATHS
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


# Abstracted Functions

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
