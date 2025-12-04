"""
Configuration file for Global Mart Stream Processing Pipeline
All paths, thresholds, and settings centralized here for easy management
"""
import os

# Base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# PATH CONFIGURATIONS FOR ENTIRE PIPELINE
PATHS = {
    # Main data directories
    'base_dir': os.path.join(BASE_DIR, 'data'),
    'clean_data': os.path.join(BASE_DIR, 'data', 'clean'),
    'quarantine': os.path.join(BASE_DIR, 'data', 'quarantine'),
    'metrics': os.path.join(BASE_DIR, 'data', 'metrics'),
    'checkpoints': os.path.join(BASE_DIR, 'data', 'checkpoints'),

    # Clean data subdirectories - will store validated/cleaned records
    'clean_users': os.path.join(BASE_DIR, 'data', 'clean', 'users'),
    'clean_products': os.path.join(BASE_DIR, 'data', 'clean', 'products'),
    'clean_views': os.path.join(BASE_DIR, 'data', 'clean', 'views'),
    'clean_carts': os.path.join(BASE_DIR, 'data', 'clean', 'carts'),
    'clean_transactions': os.path.join(BASE_DIR, 'data', 'clean', 'transactions'),

    # Quarantine subdirectories - for rejected/error records
    'quarantine_users': os.path.join(BASE_DIR, 'data', 'quarantine', 'users'),
    'quarantine_products': os.path.join(BASE_DIR, 'data', 'quarantine', 'products'),
    'quarantine_views': os.path.join(BASE_DIR, 'data', 'quarantine', 'views'),
    'quarantine_carts': os.path.join(BASE_DIR, 'data', 'quarantine', 'carts'),
    'quarantine_transactions': os.path.join(BASE_DIR, 'data', 'quarantine', 'transactions'),

    # Checkpoint subdirectories - for streaming checkpoints
    'checkpoint_users': os.path.join(BASE_DIR, 'data', 'checkpoints', 'users'),
    'checkpoint_products': os.path.join(BASE_DIR, 'data', 'checkpoints', 'products'),
    'checkpoint_views': os.path.join(BASE_DIR, 'data', 'checkpoints', 'views'),
    'checkpoint_carts': os.path.join(BASE_DIR, 'data', 'checkpoints', 'carts'),
    'checkpoint_transactions': os.path.join(BASE_DIR, 'data', 'checkpoints', 'transactions'),
    'checkpoint_alerts': os.path.join(BASE_DIR, 'data', 'checkpoints', 'alerts'),
    'checkpoint_metrics': os.path.join(BASE_DIR, 'data', 'checkpoints', 'metrics'),

    # Metrics output directories - for storing computed metrics
    'metrics_sales': os.path.join(BASE_DIR, 'data', 'metrics', 'sales'),
    'metrics_inventory': os.path.join(BASE_DIR, 'data', 'metrics', 'inventory'),
    'metrics_anomalies': os.path.join(BASE_DIR, 'data', 'metrics', 'anomalies'),
    'metrics_cart_abandonment': os.path.join(BASE_DIR, 'data', 'metrics', 'cart_abandonment'),

    # Alert logs
    'alerts_log': os.path.join(BASE_DIR, 'data', 'alerts.log'),
}

# KAFKA CONFIGURATION
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9092',
    'topics': {
        'users': 'globalmart.users',
        'products': 'globalmart.product_catalog',
        'views': 'globalmart.product_views',
        'carts': 'globalmart.cart_events',
        'transactions': 'globalmart.transaction_events',
    },
    'consumer_group_prefix': 'globalmart-stream-processor',
}

# Setting Anomaly Detection Thresholds and Rules
ANOMALY_DETECTION = {
    # Transaction-based anomalies
    'transaction': {
        'max_amount': 5000.0,  # TODO: Review - Flag transactions above this amount
        'min_amount': 0.01,    # TODO: Review - Flag transactions below this amount (potential data errors)
        'frequency_window_seconds': 300,  # TODO: Review - Time window for frequency analysis (5 minutes)
        'max_transactions_per_window': 10,  # TODO: Review - Max transactions per user in the time window
        'velocity_check_enabled': True,  # Enable velocity-based fraud detection
    },

    # Inventory-based alerts
    'inventory': {
        'low_stock_threshold': 50,  # TODO: Review - Alert when inventory drops below this
        'critical_stock_threshold': 10,  # TODO: Review - Critical alert threshold
        'out_of_stock_alert': True,  # Alert on out-of-stock items
    },

    # Cart abandonment detection
    'cart_abandonment': {
        'timeout_minutes': 30,  # TODO: Review - Consider cart abandoned after this time
        'min_cart_value': 50.0,  # TODO: Review - Only track abandonment for carts above this value
        'track_high_value_carts': True,
    },

    # Session-based anomalies
    'session': {
        'max_products_viewed': 100,  # TODO: Review - Flag if user views too many products (bot detection)
        'session_timeout_minutes': 60,  # TODO: Review - Session expires after this
    }
}

# Rules for Data Quality Checks - records will be corrected, flagged or rejected
DATA_QUALITY = {
    # Fixable errors - 
    'fixable_errors': [
        'type_conversion',      # Try to convert data types (e.g., "123" -> 123)
        'trim_whitespace',      # Remove leading/trailing spaces
        'format_dates',         # Standardize date formats
        'replace_corrupted',    # TODO: Replace "<corrupted>" with NULL
    ],

    # Reject/quarantine conditions
    'reject_conditions': [
        'missing_required_field',  # Required field is None or missing
        'negative_value',          # Negative values where not allowed (age, amount, etc.)
        'invalid_domain_value',    # Value outside acceptable domain
        'invalid_email',           # Malformed email
        'invalid_date',            # Cannot parse date
    ],

    # Required fields per data type
    'required_fields': {
        'user': ['user_id', 'email', 'age', 'country', 'registration_date'],
        'product': ['product_id', 'price', 'inventory', 'category'],
        'view': ['event_id', 'product_id', 'user_id', 'timestamp'],
        'cart': ['cart_id', 'user_id', 'timestamp', 'products'],
        'transaction': ['transaction_id', 'user_id', 'timestamp', 'products', 'total_amount', 'payment_method'],
    },

    # Valid domains/ranges
    'valid_domains': {
        'age': (18, 120),  # Valid age range
        'price': (0.01, 10000.0),  # Valid price range
        'total_amount': (0.01, 100000.0),  # Valid transaction amount
        'inventory': (0, 1000000),  # Valid inventory range
        'countries': ['USA', 'Canada', 'UK', 'Germany', 'France'], 
        'payment_methods': ['credit_card', 'paypal', 'gift_card'],
    }
}

# Spark Configuration
SPARK_CONFIG = {
    'app_name': 'Global Mart Stream Processing',
    'packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
    'log_level': 'ERROR',

    # Streaming configurations
    'trigger_interval': '10 seconds',  # TODO: Review - How often to process micro-batches
    'max_offsets_per_trigger': 10000,  # TODO: Review - Max records to process per batch

    # Performance tuning
    'shuffle_partitions': 8,  # TODO: Review - Adjust based on cluster size
}

# Alert Configuration
ALERT_CONFIG = {
    'enabled': True,
    'alert_types': {
        'low_inventory': True,
        'transaction_anomaly': True,
        'cart_abandonment': True,
        'data_quality_issues': True,
    },
    'notification_methods': [
        'console',  # Print to console
        'file',     # Write to log file
        # 'email',  # TODO: Configure email alerts
        # 'slack',  # TODO: Configure Slack webhook
    ],
    'batch_alerts': True,  # Batch similar alerts to reduce noise
    'alert_cooldown_seconds': 60,  # TODO: Review - Minimum time between similar alerts
}

# Dashboard Configuration
DASHBOARD_CONFIG = {
    'enabled': True,
    'update_interval_seconds': 10,  # TODO: Review - How often to update metrics
    'metrics_retention_hours': 24,  # TODO: Review - How long to keep real-time metrics
    'export_format': 'parquet',  # Format for metrics export
}

def create_directories():
    """Create all necessary directories if they don't exist"""
    for key, path in PATHS.items():
        if key.endswith('_log'):
            # Create parent directory for log files
            os.makedirs(os.path.dirname(path), exist_ok=True)
        else:
            # Create directory
            os.makedirs(path, exist_ok=True)
    print("✓ All directories created/verified")

if __name__ == "__main__":
    # Test: create directories and print configuration
    create_directories()
    print("\n" + "="*60)
    print("CONFIGURATION SUMMARY")
    print("="*60)
    print(f"\nBase Directory: {BASE_DIR}")
    print(f"\nKafka Bootstrap: {KAFKA_CONFIG['bootstrap_servers']}")
    print(f"\nTransaction Anomaly Threshold: ${ANOMALY_DETECTION['transaction']['max_amount']}")
    print(f"Low Stock Threshold: {ANOMALY_DETECTION['inventory']['low_stock_threshold']} units")
    print(f"Cart Abandonment Timeout: {ANOMALY_DETECTION['cart_abandonment']['timeout_minutes']} minutes")
    print("\n" + "="*60)
