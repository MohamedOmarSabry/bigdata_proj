# # iot_consumer.py
# from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
# from kafka.errors import KafkaError
# import json
# import time
# from collections import defaultdict
# import traceback

# class IoTConsumer:
#     def __init__(self, topics, group_id='iot-analytics'):
#         """Initialize consumer with configuration"""
#         self.consumer = KafkaConsumer(
#             *topics,
#             bootstrap_servers=['localhost:9092'],
#             group_id=group_id,
#             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#             key_deserializer=lambda k: k.decode('utf-8') if k else None,
#             auto_offset_reset='latest',
#             enable_auto_commit=False,  # Manual commit for exactly-once
#             max_poll_records=100,
#             session_timeout_ms=30000,
#             heartbeat_interval_ms=10000
#         )
        
#         self.metrics = defaultdict(list)
#         self.alert_thresholds = {
#             'temperature': (15, 30),
#             'humidity': (30, 70),
#             'pressure': (1000, 1030),
#             'battery_level': (20, 100),
#         }
    
#     def process_message(self, message):
#         """Process individual sensor reading"""
#         data = message.value
#         sensor_type = data['sensor_type']
#         value = data['value']
        
#         # Store metrics
#         self.metrics[sensor_type].append(value)
        
#         # Check for alerts
#         if sensor_type in self.alert_thresholds:
#             min_val, max_val = self.alert_thresholds[sensor_type]
#             if value < min_val or value > max_val:
#                 self.trigger_alert(data, min_val, max_val)
        
#         # Calculate statistics every 10 readings
#         if len(self.metrics[sensor_type]) >= 10:
#             self.calculate_statistics(sensor_type)
#             self.metrics[sensor_type] = self.metrics[sensor_type][-10:]
        
#         return True
    
#     def trigger_alert(self, data, min_val, max_val):
#         """Handle threshold violations"""
#         print(f"🚨 ALERT: {data['sensor_id']} - {data['sensor_type']} = {data['value']} - Battery Level = {data['battery_level']}"
#               f"(sensor threshold: {min_val}-{max_val}) (Battery threshold: {21}-{100})")
#         # Here you could send notifications, write to database, etc.
    
#     def calculate_statistics(self, sensor_type):
#         """Calculate running statistics"""
#         values = self.metrics[sensor_type]
#         if values:
#             avg = sum(values) / len(values)
#             min_val = min(values)
#             max_val = max(values)
#             print(f"📊 Stats for {sensor_type}: "
#                   f"Avg={avg:.2f}, Min={min_val:.2f}, Max={max_val:.2f}")
    
#     def consume_messages(self):
#         """Main consumption loop with error handling"""
#         print("🎧 Starting IoT consumer...")
        
#         try:
#             while True:
#                 # Poll for messages
#                 messages = self.consumer.poll(timeout_ms=1000)
                
#                 if messages:
#                     for topic_partition, records in messages.items():
#                         for message in records:
#                             try:
#                                 # Process message
#                                 success = self.process_message(message)
                                
#                                 if success:
#                                     # Commit offset after successful processing
#                                     # self.consumer.commit({
#                                     #     topic_partition: message.offset + 1
#                                     # })
#                                     self.consumer.commit({
#                                         TopicPartition(topic_partition.topic, topic_partition.partition):
#                                         OffsetAndMetadata(message.offset + 1, None)
#                                     })
#                             except Exception as e:
#                                 print(f"❌ Error processing message: {e}")
#                                 traceback.print_exc()
#                                 # Could write to dead letter queue here
                
#                 # Print consumer lag periodically
#                 self.print_consumer_lag()
        
#         except KeyboardInterrupt:
#             print("\n⏹️ Stopping consumer...")
#         finally:
#             self.consumer.close()
#             print("✅ Consumer stopped")
    
#     def print_consumer_lag(self):
#         """Monitor consumer lag"""
#         for partition in self.consumer.assignment():
#             committed = self.consumer.committed(partition)
#             position = self.consumer.position(partition)
#             if committed:
#                 lag = position - committed
#                 if lag > 100:
#                     print(f"⚠️ High lag on {partition}: {lag} messages")

# # Run the consumer
# if __name__ == "__main__":
#     consumer = IoTConsumer(['iot-sensors'])
#     consumer.consume_messages()