"""
Session Analysis Module
Tracks user sessions based on activity patterns (views, carts, transactions).

A session is defined as a contiguous period of activity for a user,
separated by at least session_timeout_minutes of inactivity.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_timestamp,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    countDistinct, lag, unix_timestamp, row_number, first, last,
    collect_list, size, concat_ws, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, LongType
)
from config import ANOMALY_DETECTION, KAFKA_CONFIG, PATHS
import json
import uuid
from datetime import datetime
from kafka import KafkaProducer


class SessionAnalyzer:
    """
    Analyzes user sessions by grouping activities (views, carts, transactions)
    into sessions based on inactivity gaps.
    """

    def __init__(self):
        self.session_config = ANOMALY_DETECTION['session']
        self.timeout_minutes = self.session_config['timeout_minutes']
        self._kafka_producer = None

    def get_kafka_producer(self):
        """Get or create Kafka producer"""
        if self._kafka_producer is None:
            try:
                self._kafka_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
            except Exception as e:
                print(f"Warning: Could not create Kafka producer for sessions: {e}")
        return self._kafka_producer

    def build_unified_activity_stream(self, spark: SparkSession) -> DataFrame:
        """
        Build a unified activity stream from views, carts, and transactions.
        Each activity has: user_id, timestamp, activity_type, activity_id
        """
        activities = []

        # Load views
        try:
            views_df = spark.read.parquet(PATHS['clean_views'])
            views_activity = views_df.select(
                col("user_id"),
                to_timestamp(col("timestamp")).alias("timestamp"),
                lit("view").alias("activity_type"),
                col("event_id").alias("activity_id"),
                col("product_id")
            )
            activities.append(views_activity)
        except Exception:
            pass

        # Load carts
        try:
            carts_df = spark.read.parquet(PATHS['clean_carts'])
            carts_activity = carts_df.select(
                col("user_id"),
                to_timestamp(col("timestamp")).alias("timestamp"),
                lit("cart").alias("activity_type"),
                col("cart_id").alias("activity_id"),
                lit(None).cast(StringType()).alias("product_id")
            )
            activities.append(carts_activity)
        except Exception:
            pass

        # Load transactions
        try:
            transactions_df = spark.read.parquet(PATHS['clean_transactions'])
            trans_activity = transactions_df.select(
                col("user_id"),
                to_timestamp(col("timestamp")).alias("timestamp"),
                lit("transaction").alias("activity_type"),
                col("transaction_id").alias("activity_id"),
                lit(None).cast(StringType()).alias("product_id")
            )
            activities.append(trans_activity)
        except Exception:
            pass

        if not activities:
            return None

        # Union all activities
        unified_df = activities[0]
        for df in activities[1:]:
            unified_df = unified_df.union(df)

        return unified_df

    def sessionize_activities(self, activities_df: DataFrame) -> DataFrame:
        """
        Assign session IDs to activities based on inactivity gaps.
        We first order activities by user and timestamp, then compute time gaps.
        If gap > timeout_minutes, we start a new session.
        Generate session_id is user_id + session_number.
        """
        if activities_df is None or activities_df.count() == 0:
            return None

        timeout_seconds = self.timeout_minutes * 60

        # Window for computing gaps within each user's activities
        user_window = Window.partitionBy("user_id").orderBy("timestamp")

        # Calculate previous timestamp and time gap
        with_gaps = activities_df \
            .withColumn("prev_timestamp", lag("timestamp").over(user_window)) \
            .withColumn(
                "gap_seconds",
                when(col("prev_timestamp").isNull(), lit(0))
                .otherwise(unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
            ) \
            .withColumn(
                "is_new_session",
                when(col("prev_timestamp").isNull(), lit(1))
                .when(col("gap_seconds") > timeout_seconds, lit(1))
                .otherwise(lit(0))
            )

        # Create session number by cumulative sum of is_new_session flags
        with_session_num = with_gaps.withColumn(
            "session_num",
            spark_sum("is_new_session").over(user_window)
        )

        # Create unique session_id
        sessionized = with_session_num.withColumn(
            "session_id",
            concat_ws("_", col("user_id"), col("session_num").cast(StringType()))
        )

        return sessionized

    def compute_session_metrics(self, sessionized_df: DataFrame) -> DataFrame:
        """
        Compute metrics for each session such as duration, activity counts,
        unique products viewed, conversion status, etc.
        """
        if sessionized_df is None:
            return None

        session_metrics = sessionized_df.groupBy("session_id", "user_id") \
            .agg(
                # Time metrics
                spark_min("timestamp").alias("session_start"),
                spark_max("timestamp").alias("session_end"),

                # Activity counts
                count("activity_id").alias("activity_count"),
                spark_sum(when(col("activity_type") == "view", 1).otherwise(0)).alias("view_count"),
                spark_sum(when(col("activity_type") == "cart", 1).otherwise(0)).alias("cart_count"),
                spark_sum(when(col("activity_type") == "transaction", 1).otherwise(0)).alias("transaction_count"),

                # Product diversity (only from views that have product_id)
                countDistinct(
                    when(col("product_id").isNotNull(), col("product_id"))
                ).alias("unique_products_viewed"),

                # Collect activity types for session path analysis
                collect_list("activity_type").alias("activity_sequence")
            )

        # Calculate duration and conversion status
        session_metrics = session_metrics \
            .withColumn(
                "duration_seconds",
                unix_timestamp("session_end") - unix_timestamp("session_start")
            ) \
            .withColumn(
                "converted",
                col("transaction_count") > 0
            ) \
            .withColumn(
                "added_to_cart",
                col("cart_count") > 0
            ) \
            .withColumn(
                "session_type",
                when(col("transaction_count") > 0, lit("converted"))
                .when(col("cart_count") > 0, lit("cart_abandoned"))
                .when(col("view_count") > 0, lit("browse_only"))
                .otherwise(lit("unknown"))
            )

        return session_metrics

    def compute_aggregate_session_stats(self, session_metrics_df: DataFrame) -> dict:
        """
        Compute aggregate statistics across all sessions for dashboard summary.
        """
        if session_metrics_df is None or session_metrics_df.count() == 0:
            return {}

        stats = session_metrics_df.agg(
            count("session_id").alias("total_sessions"),
            countDistinct("user_id").alias("unique_users"),
            avg("duration_seconds").alias("avg_session_duration"),
            avg("activity_count").alias("avg_activities_per_session"),
            avg("view_count").alias("avg_views_per_session"),
            spark_sum(when(col("converted"), 1).otherwise(0)).alias("converted_sessions"),
            spark_sum(when(col("added_to_cart") & ~col("converted"), 1).otherwise(0)).alias("cart_abandoned_sessions")
        ).collect()[0]

        total_sessions = stats["total_sessions"] or 0
        converted = stats["converted_sessions"] or 0
        cart_abandoned = stats["cart_abandoned_sessions"] or 0

        return {
            "total_sessions": int(total_sessions),
            "unique_users": int(stats["unique_users"] or 0),
            "avg_session_duration_seconds": float(stats["avg_session_duration"] or 0),
            "avg_activities_per_session": float(stats["avg_activities_per_session"] or 0),
            "avg_views_per_session": float(stats["avg_views_per_session"] or 0),
            "conversion_rate": float(converted / total_sessions * 100) if total_sessions > 0 else 0,
            "cart_abandonment_rate": float(cart_abandoned / total_sessions * 100) if total_sessions > 0 else 0,
            "converted_sessions": int(converted),
            "cart_abandoned_sessions": int(cart_abandoned)
        }


def process_sessions_batch(batch_df, batch_id):
    """
    Process sessions for the current state of all activities.
    Triggered by new view events, but analyzes all stored activities.
    """
    from pyspark.sql.functions import to_json, struct

    spark = batch_df.sparkSession
    analyzer = SessionAnalyzer()

    # Build unified activity stream from all stored data
    activities_df = analyzer.build_unified_activity_stream(spark)

    if activities_df is None or activities_df.count() == 0:
        print(f"[Batch {batch_id}] Sessions: No activity data available yet")
        return

    # Sessionize activities
    sessionized_df = analyzer.sessionize_activities(activities_df)

    if sessionized_df is None:
        print(f"[Batch {batch_id}] Sessions: Could not sessionize activities")
        return

    # Compute session metrics
    session_metrics = analyzer.compute_session_metrics(sessionized_df)

    if session_metrics is None or session_metrics.count() == 0:
        print(f"[Batch {batch_id}] Sessions: No sessions computed")
        return

    # Get aggregate stats
    aggregate_stats = analyzer.compute_aggregate_session_stats(session_metrics)

    # Prepare output for Kafka - convert activity_sequence array to string
    output_df = session_metrics \
        .withColumn("activity_path", concat_ws("->", col("activity_sequence"))) \
        .drop("activity_sequence") \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("processed_at", current_timestamp())

    # Write individual sessions to Kafka (for recent sessions display)
    json_df = output_df.select(
        to_json(struct(*[col(c) for c in output_df.columns])).alias("value")
    )
    json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", KAFKA_CONFIG['topics']['metrics_sessions']) \
        .save()

    # Also send aggregate summary to a separate message for accurate totals
    # This ensures the dashboard has accurate stats regardless of deque size
    producer = analyzer.get_kafka_producer()
    if producer:
        summary_message = {
            "message_type": "session_summary",
            "batch_id": batch_id,
            "processed_at": datetime.now().isoformat(),
            **aggregate_stats
        }
        producer.send(KAFKA_CONFIG['topics']['metrics_sessions'], value=summary_message)
        producer.flush()

    session_count = session_metrics.count()
    print(f"[Batch {batch_id}] Sessions: {session_count} sessions analyzed")
    print(f"    Conversion rate: {aggregate_stats.get('conversion_rate', 0):.1f}%")
    print(f"    Avg duration: {aggregate_stats.get('avg_session_duration_seconds', 0):.0f}s")
    print(f"    Avg views/session: {aggregate_stats.get('avg_views_per_session', 0):.1f}")
