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
    'base_dir': os.path.join(BASE_DIR, 'Staging'),
    'clean_data': os.path.join(BASE_DIR, 'Staging', 'clean'),
    'quarantine': os.path.join(BASE_DIR, 'Staging', 'quarantine'),
    'checkpoints': os.path.join(BASE_DIR, 'Staging', 'checkpoints'),
    
    # Clean data subdirectories - will store validated/cleaned records
    'clean_users': os.path.join(BASE_DIR, 'Staging', 'clean', 'users'),
    'clean_products': os.path.join(BASE_DIR, 'Staging', 'clean', 'products'),
    'clean_views': os.path.join(BASE_DIR, 'Staging', 'clean', 'views'),
    'clean_carts': os.path.join(BASE_DIR, 'Staging', 'clean', 'carts'),
    'clean_transactions': os.path.join(BASE_DIR, 'Staging', 'clean', 'transactions'),

    # Quarantine subdirectories - for rejected/error records
    'quarantine_users': os.path.join(BASE_DIR, 'Staging', 'quarantine', 'users'),
    'quarantine_products': os.path.join(BASE_DIR, 'Staging', 'quarantine', 'products'),
    'quarantine_views': os.path.join(BASE_DIR, 'Staging', 'quarantine', 'views'),
    'quarantine_carts': os.path.join(BASE_DIR, 'Staging', 'quarantine', 'carts'),
    'quarantine_transactions': os.path.join(BASE_DIR, 'Staging', 'quarantine', 'transactions'),

    # Checkpoint subdirectories - for streaming checkpoints
    'checkpoint_users': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'users'),
    'checkpoint_products': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'products'),
    'checkpoint_views': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'views'),
    'checkpoint_carts': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'carts'),
    'checkpoint_transactions': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'transactions'),
    # 'checkpoint_alerts': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'alerts'),
    # 'checkpoint_metrics': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'metrics'),
    'checkpoint_sales_aggregation': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'sales_aggregation'),
    'checkpoint_cart_abandonment': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'cart_abandonment'),
    'checkpoint_transaction_anomalies': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'transaction_anomalies'),
    'checkpoint_inventory_alerts': os.path.join(BASE_DIR, 'Staging', 'checkpoints', 'inventory_alerts'),

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
        
        # Real-time metrics streams to be consumed by dashboard
        'metrics_sales_hourly': 'globalmart.metrics.sales_hourly',
        'metrics_sales_category': 'globalmart.metrics.sales_category',
        'metrics_sales_country': 'globalmart.metrics.sales_country',
        'metrics_anomalies': 'globalmart.metrics.anomalies',
        'metrics_inventory_alerts': 'globalmart.metrics.inventory_alerts',
        'metrics_abandoned_carts': 'globalmart.metrics.abandoned_carts',
        'alerts': 'globalmart.alerts',
    },
}

# Setting Anomaly Detection Thresholds and Rules
ANOMALY_DETECTION = {
    # Transaction-based anomalies
    'transaction': {
        'max_amount': 15000.0,
        'min_amount': 0.01,
        # 'max_transactions_per_window': 10,  # TODO: Review - Max transactions per user in the time window
        # 'velocity_check_enabled': True,  # Enable velocity-based fraud detection
    },

    # Inventory-based alerts
    'inventory': {
        'low_stock_threshold': 50,
        'critical_stock_threshold': 10,
        'out_of_stock_alert': True,
    },

    # Cart abandonment detection
    'cart_abandonment': {
        'timeout_minutes': 15,
        'min_cart_value': 2000.0,
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
        'replace_corrupted',
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
    'packages': 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1',
    'log_level': 'ERROR',

    # Streaming configurations
    'trigger_interval': '10 seconds',
    'max_offsets_per_trigger': 10000,

    # Performance tuning
    'shuffle_partitions': 8,
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
        'file'     # Write to log file
    ],
    'batch_alerts': True,  # Batch similar alerts to reduce noise
    'alert_cooldown_seconds': 60,
}

# Dashboard Configuration
DASHBOARD_CONFIG = {
    'enabled': True,
    'update_interval_seconds': 5,
    'metrics_retention_hours': 24, # REVIEW - what is the difference between this and time window hours?
    'time_window_hours': 24,
    'export_format': 'parquet', # REVIEW - we dont need to export?
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
    create_directories()
