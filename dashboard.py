"""
Real-Time Analytics Dashboard
Displays metrics and alerts from Kafka streaming topics
"""
import os
import sys
import json
from threading import Thread, Lock
import time

# Set SPARK_HOME to use PySpark from virtual environment
pyspark_path = os.path.join(sys.prefix, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages', 'pyspark')
os.environ['SPARK_HOME'] = pyspark_path

from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
from config import KAFKA_CONFIG, PATHS, DASHBOARD_CONFIG
from datetime import datetime, timedelta
from collections import deque

app = Flask(__name__)

# Cumulative counters for metrics (monotonically increasing)
metrics_counters = {
    'total_anomalies': 0,
    'total_inventory_alerts': 0,
    'total_abandoned_carts': 0,
    'total_sales': 0.0,
    'total_transactions': 0,
}

# Bounded deques for storing recent event details (for display)
recent_events = {
    'sales_hourly': deque(maxlen=100),      # ~100 batches worth of hourly metrics
    'sales_category': deque(maxlen=100),    # ~100 batches worth of category metrics
    'sales_country': deque(maxlen=100),     # ~100 batches worth of country metrics (was 5, causing countries to disappear!)
    'anomalies': deque(maxlen=100),         # Recent transaction records for anomaly display
    'inventory_alerts': deque(maxlen=100),  # Recent product records for inventory display
    'abandoned_carts': deque(maxlen=50),    # Recent abandoned carts
    'alerts': deque(maxlen=100),            # General alerts from all sources
}

# Lock for thread-safe cache access
cache_lock = Lock()

# Kafka consumer threads
consumer_threads = []
stop_consumers = False


def consume_kafka_topic(topic_name, cache_key):
    """
    Background thread that consumes messages from a Kafka topic
    and stores them in the metrics cache
    """
    global stop_consumers

    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            auto_offset_reset='latest',  # Start from latest messages
            enable_auto_commit=True,
            group_id=f'dashboard-{cache_key}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print(f"✓ Kafka consumer started for {topic_name}")

        for message in consumer:
            if stop_consumers:
                break

            try:
                # Parse the message value
                data = message.value

                # Add timestamp if not present
                if 'consumed_at' not in data:
                    data['consumed_at'] = datetime.now().isoformat()

                # Add to cache and update counters (thread-safe)
                with cache_lock:
                    recent_events[cache_key].append(data)

                    # Increment counters based on event type
                    if cache_key == 'anomalies' and data.get('is_anomaly'):
                        metrics_counters['total_anomalies'] += 1
                    elif cache_key == 'inventory_alerts' and data.get('needs_alert'):
                        metrics_counters['total_inventory_alerts'] += 1
                    elif cache_key == 'abandoned_carts':
                        metrics_counters['total_abandoned_carts'] += 1
                    elif cache_key == 'sales_hourly':
                        metrics_counters['total_sales'] += float(data.get('total_sales', 0))
                        metrics_counters['total_transactions'] += int(data.get('transaction_count', 0))

            except Exception as e:
                print(f"Error processing message from {topic_name}: {e}")

        consumer.close()

    except Exception as e:
        print(f"Error in Kafka consumer for {topic_name}: {e}")


def start_kafka_consumers():
    """Start all Kafka consumer threads"""
    global consumer_threads

    # Map of topics to cache keys
    topic_mappings = {
        KAFKA_CONFIG['topics']['metrics_sales_hourly']: 'sales_hourly',
        KAFKA_CONFIG['topics']['metrics_sales_category']: 'sales_category',
        KAFKA_CONFIG['topics']['metrics_sales_country']: 'sales_country',
        KAFKA_CONFIG['topics']['metrics_anomalies']: 'anomalies',
        KAFKA_CONFIG['topics']['metrics_inventory_alerts']: 'inventory_alerts',
        KAFKA_CONFIG['topics']['metrics_abandoned_carts']: 'abandoned_carts',
        KAFKA_CONFIG['topics']['alerts']: 'alerts',
    }

    # Start a consumer thread for each topic
    for topic_name, cache_key in topic_mappings.items():
        thread = Thread(target=consume_kafka_topic, args=(topic_name, cache_key), daemon=True)
        thread.start()
        consumer_threads.append(thread)

    print(f"✓ Started {len(consumer_threads)} Kafka consumer threads")


def apply_time_filter(data_list, time_window_hours):
    """Filter data based on time window"""
    if time_window_hours == 0:
        return data_list

    cutoff_time = datetime.now() - timedelta(hours=time_window_hours)

    filtered = []
    for item in data_list:
        # Try to find a timestamp field
        timestamp = None
        for key in ['processed_at', 'consumed_at', 'timestamp', 'detected_at']:
            if key in item:
                try:
                    # Parse timestamp and make it naive (remove timezone info)
                    ts = datetime.fromisoformat(item[key].replace('Z', '+00:00'))
                    # Convert to naive datetime for comparison
                    timestamp = ts.replace(tzinfo=None) if ts.tzinfo else ts
                    break
                except:
                    continue

        # Include item if within time window or no timestamp found
        if timestamp is None or timestamp >= cutoff_time:
            filtered.append(item)

    return filtered


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/api/metrics/summary')
def get_metrics_summary():
    """Get summary of all metrics using stable counters"""
    try:
        with cache_lock:
            # Use stable counters for metrics (not affected by cache limits)
            anomaly_count = metrics_counters['total_anomalies']
            low_stock_count = metrics_counters['total_inventory_alerts']
            abandoned_count = metrics_counters['total_abandoned_carts']
            total_sales = metrics_counters['total_sales']
            total_transactions = metrics_counters['total_transactions']

        return jsonify({
            'anomaly_count': anomaly_count,
            'low_stock_count': low_stock_count,
            'abandoned_carts_count': abandoned_count,
            'total_sales': round(total_sales, 2),
            'total_transactions': total_transactions,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Error in metrics summary: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/recent')
def get_recent_alerts():
    """Get recent alerts from Kafka stream"""
    try:
        with cache_lock:
            # Get alerts from recent events cache
            alerts = list(recent_events['alerts'])

        # Sort by timestamp (newest first)
        alerts.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

        return jsonify({
            'recent_events': alerts,
            'count': len(alerts)
        })
    except Exception as e:
        print(f"Error in recent alerts: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/sales/hourly')
def get_hourly_sales():
    """Get hourly sales metrics from Kafka stream"""
    try:
        time_window_hours = DASHBOARD_CONFIG.get('time_window_hours', 0)

        with cache_lock:
            data = list(recent_events['sales_hourly'])

        # Apply time filter
        data = apply_time_filter(data, time_window_hours)

        # Aggregate by hour (sum across all entries for each hour)
        hourly_agg = {}
        for item in data:
            hour = item.get('hour')
            if hour is not None:
                if hour not in hourly_agg:
                    hourly_agg[hour] = {
                        'hour': hour,
                        'total_sales': 0,
                        'transaction_count': 0,
                        'unique_customers': 0,
                        'avg_transaction_value': 0,
                        'sales_velocity': 0
                    }

                hourly_agg[hour]['total_sales'] += float(item.get('total_sales', 0))
                hourly_agg[hour]['transaction_count'] += int(item.get('transaction_count', 0))
                hourly_agg[hour]['unique_customers'] += int(item.get('unique_customers', 0))
                hourly_agg[hour]['sales_velocity'] = hourly_agg[hour]['total_sales'] / 3600

        # Calculate average transaction value
        for hour, agg in hourly_agg.items():
            if agg['transaction_count'] > 0:
                agg['avg_transaction_value'] = agg['total_sales'] / agg['transaction_count']

        # Convert to list and sort by hour
        result = sorted(list(hourly_agg.values()), key=lambda x: x['hour'])

        return jsonify({
            'data': result,
            'count': len(result)
        })
    except Exception as e:
        print(f"Error in hourly sales: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/sales/category')
def get_category_sales():
    """Get sales by category from Kafka stream"""
    try:
        time_window_hours = DASHBOARD_CONFIG.get('time_window_hours', 0)

        with cache_lock:
            data = list(recent_events['sales_category'])

        # Apply time filter
        data = apply_time_filter(data, time_window_hours)

        # Aggregate by category
        category_agg = {}
        for item in data:
            category = item.get('category')
            if category and category != 'null':
                if category not in category_agg:
                    category_agg[category] = {
                        'category': category,
                        'total_revenue': 0,
                        'total_units_sold': 0,
                        'transaction_count': 0,
                        'unique_products': 0,
                        'total_unique_customers': 0,
                        'avg_product_price': 0
                    }

                category_agg[category]['total_revenue'] += float(item.get('total_revenue', 0))
                category_agg[category]['total_units_sold'] += int(item.get('total_units_sold', 0))
                category_agg[category]['transaction_count'] += int(item.get('transaction_count', 0))
                category_agg[category]['unique_products'] += int(item.get('unique_products', 0))
                category_agg[category]['total_unique_customers'] += int(item.get('total_unique_customers', 0))

        # Calculate average product price
        for category, agg in category_agg.items():
            if agg['total_units_sold'] > 0:
                agg['avg_product_price'] = agg['total_revenue'] / agg['total_units_sold']

        # Convert to list and sort by revenue
        result = sorted(list(category_agg.values()), key=lambda x: x['total_revenue'], reverse=True)

        return jsonify({
            'data': result,
            'count': len(result)
        })
    except Exception as e:
        print(f"Error in category sales: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/sales/country')
def get_country_sales():
    """Get sales by country from Kafka stream"""
    try:
        time_window_hours = DASHBOARD_CONFIG.get('time_window_hours', 0)

        with cache_lock:
            data = list(recent_events['sales_country'])

        # Apply time filter
        data = apply_time_filter(data, time_window_hours)

        # Aggregate by country
        country_agg = {}
        for item in data:
            country = item.get('country')
            if country and country != 'null':
                if country not in country_agg:
                    country_agg[country] = {
                        'country': country,
                        'total_sales': 0,
                        'transaction_count': 0,
                        'unique_customers': 0,
                        'avg_transaction_value': 0,
                        'sales_percentage': 0
                    }

                country_agg[country]['total_sales'] += float(item.get('total_sales', 0))
                country_agg[country]['transaction_count'] += int(item.get('transaction_count', 0))
                country_agg[country]['unique_customers'] += int(item.get('unique_customers', 0))

        # Calculate average transaction value and sales percentage
        total_sales = sum(agg['total_sales'] for agg in country_agg.values())
        for country, agg in country_agg.items():
            if agg['transaction_count'] > 0:
                agg['avg_transaction_value'] = agg['total_sales'] / agg['transaction_count']
            if total_sales > 0:
                agg['sales_percentage'] = (agg['total_sales'] / total_sales) * 100

        # Convert to list and sort by sales
        result = sorted(list(country_agg.values()), key=lambda x: x['total_sales'], reverse=True)

        return jsonify({
            'data': result,
            'count': len(result)
        })
    except Exception as e:
        print(f"Error in country sales: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/anomalies')
def get_anomalies():
    """Get transaction anomalies from Kafka stream"""
    try:
        time_window_hours = DASHBOARD_CONFIG.get('time_window_hours', 0)

        with cache_lock:
            data = list(recent_events['anomalies'])
            total_count = metrics_counters['total_anomalies']

        # Apply time filter
        data = apply_time_filter(data, time_window_hours)

        # Filter only anomalies for display
        anomalies = [d for d in data if d.get('is_anomaly')]

        return jsonify({
            'count': total_count,  # Stable counter
            'recent_events': anomalies  # Recent events for display
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/inventory/alerts')
def get_inventory_alerts():
    """Get inventory alerts from Kafka stream"""
    try:
        time_window_hours = DASHBOARD_CONFIG.get('time_window_hours', 0)

        with cache_lock:
            data = list(recent_events['inventory_alerts'])
            total_count = metrics_counters['total_inventory_alerts']

        # Apply time filter
        data = apply_time_filter(data, time_window_hours)

        # Filter only items that need alerts for display
        alerts = [d for d in data if d.get('needs_alert')]

        return jsonify({
            'count': total_count,  # Stable counter
            'recent_events': alerts  # Recent events for display
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/carts/abandoned')
def get_abandoned_carts():
    """Get abandoned carts from Kafka stream"""
    try:
        time_window_hours = DASHBOARD_CONFIG.get('time_window_hours', 0)

        with cache_lock:
            data = list(recent_events['abandoned_carts'])
            total_count = metrics_counters['total_abandoned_carts']

        # Apply time filter for display
        data = apply_time_filter(data, time_window_hours)

        return jsonify({
            'count': total_count,  # Stable counter
            'recent_events': data  # Recent events for display
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("\n" + "="*60)
    print("Starting Real-Time Analytics Dashboard")
    print("="*60)
    print("\n Dashboard URL: http://localhost:5001")
    print(" Press Ctrl+C to stop\n")

    # Start Kafka consumers in background threads
    start_kafka_consumers()

    # Start Flask app
    try:
        app.run(host='0.0.0.0', port=5001, debug=True, use_reloader=False)
    except KeyboardInterrupt:
        stop_consumers = True
        print("\n\n⚠ Shutting down dashboard...")
