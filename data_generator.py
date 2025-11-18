# # iot_producer.py
# from kafka import KafkaProducer
# import json
# import time
# import random
# from datetime import datetime
# import threading

# class IoTSensorProducer:
#     def __init__(self, bootstrap_servers='localhost:9092'):
#         """Initialize Kafka producer with configuration"""
#         self.producer = KafkaProducer(
#             bootstrap_servers=bootstrap_servers,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#             key_serializer=lambda k: k.encode('utf-8') if k else None,
#             # Producer configurations for reliability
#             acks='all',  # Wait for all replicas
#             retries=3,
#             max_in_flight_requests_per_connection=1,
#             compression_type='gzip'
#         )
        
#         self.sensor_types = ['temperature', 'humidity', 'pressure', 'motion']
#         self.locations = ['building_a', 'building_b', 'building_c']
#         self.running = True
    
#     def generate_sensor_data(self, sensor_id):
#         """Generate realistic sensor readings"""
#         sensor_type = self.sensor_types[sensor_id % len(self.sensor_types)]
#         location = self.locations[sensor_id % len(self.locations)]
        
#         # Generate realistic values based on sensor type
#         if sensor_type == 'temperature':
#             value = 20 + random.gauss(0, 5)  # Celsius
#             unit = 'celsius'
#         elif sensor_type == 'humidity':
#             value = 50 + random.gauss(0, 15)  # Percentage
#             unit = 'percent'
#         elif sensor_type == 'pressure':
#             value = 1013 + random.gauss(0, 10)  # hPa
#             unit = 'hPa'
#         else:  # motion
#             value = random.choice([0, 1])  # Binary
#             unit = 'boolean'
        
#         return {
#             'sensor_id': f'sensor_{sensor_id}',
#             'sensor_type': sensor_type,
#             'location': location,
#             'value': round(value, 2),
#             'unit': unit,
#             'timestamp': datetime.now().isoformat(),
#             'quality': random.choice(['good', 'good', 'good', 'degraded']),
#             'battery_level': round(random.uniform(10.0, 100.0), 2),
#             'signal_strength': round(random.uniform(1.0, 5.0), 2) ,  
#         }
    
#     def produce_sensor_stream(self, sensor_id, interval=1):
#         """Continuously produce sensor data"""
#         while self.running:
#             try:
#                 data = self.generate_sensor_data(sensor_id)
                
#                 # Send to Kafka with partition key for ordering
#                 self.producer.send(
#                     topic='iot-sensors',
#                     key=data['sensor_id'],
#                     value=data,
#                     timestamp_ms=int(time.time() * 1000)
#                 )
                
#                 print(f"📡 Sent: {data['sensor_id']} - {data['sensor_type']}: {data['value']} - Battery Level: {data['battery_level']} - Signal Strength: {data['signal_strength']}")
#                 time.sleep(interval + random.uniform(-0.5, 0.5))
#             except Exception as e:
#                 print(f"❌ Error producing data: {e}")
#                 time.sleep(5)
    
#     def start_simulation(self, num_sensors=10):
#         """Start multi-threaded sensor simulation"""
#         print(f"🚀 Starting IoT simulation with {num_sensors} sensors...")
        
#         # Create topic if not exists
#         from kafka.admin import KafkaAdminClient, NewTopic
#         admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
#         try:
#             topic = NewTopic(name='iot-sensors', num_partitions=3, replication_factor=1)
#             admin.create_topics([topic])
#             print("✅ Created topic 'iot-sensors'")
#         except:
#             print("ℹ️ Topic 'iot-sensors' already exists")
        
#         # Start producer threads
#         threads = []
#         for i in range(num_sensors):
#             thread = threading.Thread(
#                 target=self.produce_sensor_stream,
#                 args=(i, random.uniform(0.5, 2))
#                 #args=(i, random.uniform(5, 10))
#             )
#             thread.start()
#             threads.append(thread)
        
#         try:
#             # Run for specified duration
#             time.sleep(60)  # Run for 1 minute
#         except KeyboardInterrupt:
#             print("\n⏹️ Stopping simulation...")
#         finally:
#             self.running = False
#             for thread in threads:
#                 thread.join()
#             self.producer.close()
#             print("✅ Simulation stopped")

# # Run the producer
# if __name__ == "__main__":
#     producer = IoTSensorProducer()
#     producer.start_simulation(num_sensors=5)