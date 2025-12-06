"""
Kafka Metrics Exporter for Prometheus
Monitors Kafka broker health, topic metrics, and consumer lag
"""

from prometheus_client import start_http_server, Gauge, Counter, Histogram
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import time
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Prometheus metrics
kafka_broker_up = Gauge('kafka_broker_up', 'Kafka broker availability (1=up, 0=down)')
kafka_topic_partitions = Gauge('kafka_topic_partitions', 'Number of partitions per topic', ['topic'])
kafka_topic_messages = Counter('kafka_topic_messages_total', 'Total messages per topic', ['topic'])
kafka_consumer_lag = Gauge('kafka_consumer_lag', 'Consumer lag per topic and partition', ['topic', 'partition', 'consumer_group'])
kafka_topic_size_bytes = Gauge('kafka_topic_size_bytes', 'Topic size in bytes', ['topic'])
kafka_message_rate = Gauge('kafka_message_rate_per_sec', 'Message ingestion rate per second', ['topic'])

class KafkaMetricsExporter:
    def __init__(self, bootstrap_servers='localhost:9092', topics=None):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or [
            'globalmart.users',
            'globalmart.product_views',
            'globalmart.cart_events',
            'globalmart.transaction_events',
            'globalmart.product_catalog'
        ]
        self.admin_client = None
        self.consumer = None
        self.message_counts = {topic: 0 for topic in self.topics}
        self.last_check_time = time.time()
        
    def connect(self):
        """Establish connection to Kafka"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=5000
            )
            
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id='prometheus-exporter',
                consumer_timeout_ms=1000
            )
            
            kafka_broker_up.set(1)
            logger.info("Successfully connected to Kafka")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            kafka_broker_up.set(0)
            return False
    
    def collect_topic_metrics(self):
        """Collect metrics for each topic"""
        try:
            if not self.admin_client:
                return
            
            # Get topic metadata
            cluster_metadata = self.consumer.topics()
            
            for topic in self.topics:
                try:
                    # Get partition count
                    partitions = self.consumer.partitions_for_topic(topic)
                    if partitions:
                        kafka_topic_partitions.labels(topic=topic).set(len(partitions))
                        
                        # Get offset information for each partition
                        for partition in partitions:
                            try:
                                # Get high water mark (end offset)
                                end_offset = self.consumer.end_offsets([(topic, partition)])
                                if end_offset:
                                    offset_value = end_offset.get((topic, partition), 0)
                                    # This represents total messages
                                    kafka_topic_messages.labels(topic=topic).inc(0)  # Just to create the metric
                                    
                            except Exception as e:
                                logger.warning(f"Error getting offsets for {topic}:{partition}: {e}")
                                
                except Exception as e:
                    logger.warning(f"Error collecting metrics for topic {topic}: {e}")
                    
        except Exception as e:
            logger.error(f"Error collecting topic metrics: {e}")
    
    def collect_consumer_lag(self):
        """Calculate consumer lag for monitoring consumer groups"""
        try:
            if not self.admin_client:
                return
            
            # Get list of consumer groups
            consumer_groups = self.admin_client.list_consumer_groups()
            
            for group_info in consumer_groups:
                group_id = group_info[0]
                
                try:
                    # Get group offsets
                    group_offsets = self.admin_client.list_consumer_group_offsets(group_id)
                    
                    for topic_partition, offset_metadata in group_offsets.items():
                        topic = topic_partition.topic
                        partition = topic_partition.partition
                        committed_offset = offset_metadata.offset
                        
                        # Get log end offset
                        end_offsets = self.consumer.end_offsets([topic_partition])
                        end_offset = end_offsets.get(topic_partition, 0)
                        
                        # Calculate lag
                        lag = max(0, end_offset - committed_offset)
                        kafka_consumer_lag.labels(
                            topic=topic,
                            partition=str(partition),
                            consumer_group=group_id
                        ).set(lag)
                        
                except Exception as e:
                    logger.warning(f"Error collecting lag for group {group_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error collecting consumer lag: {e}")
    
    def collect_message_rates(self):
        """Calculate message ingestion rates"""
        try:
            current_time = time.time()
            time_diff = current_time - self.last_check_time
            
            if time_diff >= 1.0:  # Update every second
                for topic in self.topics:
                    try:
                        # Poll for new messages
                        messages = self.consumer.poll(timeout_ms=100, max_records=1000)
                        
                        count = 0
                        for topic_partition, records in messages.items():
                            if topic_partition.topic == topic:
                                count += len(records)
                        
                        if count > 0:
                            rate = count / time_diff
                            kafka_message_rate.labels(topic=topic).set(rate)
                            
                    except Exception as e:
                        logger.warning(f"Error calculating rate for {topic}: {e}")
                
                self.last_check_time = current_time
                
        except Exception as e:
            logger.error(f"Error collecting message rates: {e}")
    
    def collect_metrics(self):
        """Collect all Kafka metrics"""
        if not self.connect():
            time.sleep(5)
            return
        
        self.collect_topic_metrics()
        self.collect_consumer_lag()
        self.collect_message_rates()
    
    def run(self, port=9091, interval=15):
        """Start the metrics exporter server"""
        logger.info(f"Starting Kafka metrics exporter on port {port}")
        start_http_server(port)
        
        while True:
            try:
                self.collect_metrics()
                time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Shutting down exporter...")
                break
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                time.sleep(interval)
        
        # Cleanup
        if self.consumer:
            self.consumer.close()
        if self.admin_client:
            self.admin_client.close()

if __name__ == "__main__":
    exporter = KafkaMetricsExporter()
    exporter.run(port=9091, interval=10)
