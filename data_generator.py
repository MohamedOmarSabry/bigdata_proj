from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime, timedelta
import threading

class GlobalMartProducer:
    def __init__(self, bootstrap_servers='localhost:9092',config=[]):
        """Initialize Kafka producer with configuration"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # Producer configurations for reliability
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='gzip'
        )
        self.country_list=config['countries']
        format_string = "%Y-%m-%d"
        self.start_date=datetime.strptime(config['start_date'], format_string)
        self.purchase_probability=config['purchase_probability']
        self.cart_probability=config['cart_probability']
        self.products,self.category_to_products = self.generate_product_catalog()
        self.events_per_sec=config['events_per_second']
        self.user_error_rate=config.get('user_error_rate',0.01)
        self.product_view_error_rate=config.get('product_view_error_rate',0.005)
        self.cart_event_error_rate=config.get('cart_event_error_rate',0.002)
        self.transaction_event_error_rate=config.get('transaction_event_error_rate',0.001)
        self.product_error_rate=config.get('product_error_rate',0.001)
        self.running = True
    def failure_injector(self,value,fault_type='missing_field'):
        if fault_type == 'negative':
            return -abs(value)
        elif fault_type == 'missing_field':
            return None
        elif fault_type == 'invalid_value':
            if isinstance(value, (int, float)):
                return 999999  # absurd numeric
            elif isinstance(value, str):
                return "INVALID_VALUE"
        else:
            return value
    def generate_user(self):
        """Generate realistic user data"""
        user_id = f"user_{random.randint(1, 10_000_000)}"
        random.seed(user_id)
        email= f"{user_id}@example.com"
        age=random.randint(18,80)
        country= random.choice(self.country_list)
        today = datetime.today()
        days_diff = (today - self.start_date).days
        registration_date = self.start_date + timedelta(days=random.randint(0, days_diff))  #Registration dates from site start date to today
        num_categories = random.randint(1, 5)
        picked = random.sample(range(1, 101), num_categories)
        preferences = [f"category_{c}" for c in picked]
        return {
            "user_id": user_id,
            "email": email,
            "age": age,
            "country": country,
            "registration_date": registration_date,
            "preferences": preferences,
        }
    def generate_product_catalog(self):
        """Generate realistic product catalog data"""
        products = {}
        category_to_products = {}
        for pid in range(1, 250_001):
            product_id = f"product_{pid}"
            random.seed(product_id)
            category= f"category_{random.randint(1, 100)}"
            products[product_id] = {
                "price": round(random.uniform(5, 500), 2),
                "inventory": random.randint(0, 1000),
                "category": category
            }
            if category not in category_to_products:
                category_to_products[category] = []
            category_to_products[category].append(product_id) 
        return products, category_to_products
    def generate_product_view(self,user_id=None,user_preferences=[]):
        """Generate product view event data"""
        if not user_preferences or random.random() > 0.7:
            product_id = random.choice(list(self.products.keys()))
        else:
            category = random.choice(user_preferences)
            product_id = random.choice(self.category_to_products[category])
        event_id = str(uuid.uuid4())
        random.seed(event_id)
        timestamp = datetime.now().isoformat()
        return {
            "event_id": event_id,
            "product_id": product_id,
            "user_id": user_id,
            "timestamp": timestamp
        }
    def generate_cart_event(self,products=[],user_id=None):
        """Generate cart event data"""
        available = [
            pid for pid in products
            if self.products[pid]["inventory"] > 0
        ]

        if not available:
            return None  # Can't create cart event
        uuid = str(uuid.uuid4())
        cart_id = f"cart_{uuid}"
        random.seed(cart_id)
        timestamp = datetime.now().isoformat()
        cart_products = []
        for pid in available:
            max_qty = min(5, self.products[pid]["inventory"])
            quantity = random.randint(1, max_qty)
            cart_products.append({
                "product_id": pid,
                "quantity": quantity,
                "price": self.products[pid]["price"]
            })
        return {
            "cart_id": cart_id,
            "user_id": user_id,
            "timestamp": timestamp,
            "products": cart_products
        }
    def generate_transcation_event(self,user_id=None,products=[]):
        """Generate transcation event data"""
        uuid = str(uuid.uuid4())
        transaction_id = f"transaction_{uuid}"
        random.seed(transaction_id)
        timestamp = datetime.now().isoformat()
        total_amount=0
        for p in products:
            pid = p["product_id"]
            qty = p["quantity"]
            prc = p["price"]
            self.products[pid]["inventory"] -= qty
            total_amount += qty * prc
        return {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "timestamp": timestamp,
            "products": products,
            "total_amount": round(total_amount,2),
            "payment_method": random.choice(["credit_card", "paypal", "gift_card"])
        }


    #Idea generate user --> generate products --> generate product view --> generate cart event --> generate transaction event
    def produce_data_stream(self, interval=1):
        """Continuously produce sensor data"""
        interval = 1 / self.events_per_sec
        while self.running:
            try:
                user_data = self.generate_user()
                print(
                    f"User: {user_data['user_id']}- "
                    f"{user_data['email']} - {user_data['age']} - "
                    f"{user_data['registration_date']} - {user_data['preferences']}"
                )
                self.producer.send(
                    topic='globalmart.users',
                    key=user_data['user_id'],
                    value=user_data,
                    timestamp_ms=int(time.time() * 1000)
                )
                product_data_L = []
                for _ in range(random.randint(1,20)):
                    product_view_event = self.generate_product_view(user_data['user_id'],user_preferences=user_data['preferences'])
                    product_data_L.append(product_view_event['product_id'])
                    print(
                    f"Product View: {product_view_event['event_id']}- "
                    f"{product_view_event['product_id']} - {product_view_event['user_id']} - "
                    f"{product_view_event['timestamp']}"
                    )
                    self.producer.send(
                    topic='globalmart.product_views',
                    key=product_view_event['event_id'],
                    value=product_view_event,
                    timestamp_ms=int(time.time() * 1000)
                    )
                if random.random() < self.cart_probability:
                    cart_event = self.generate_cart_event(product_data_L, user_data["user_id"])
                    if cart_event and len(cart_event["products"]) > 0:
                        print(
                        f"Cart Event: {cart_event['cart_id']}- "
                        f"{cart_event['user_id']} - {cart_event['products']} - "
                        f"{cart_event['timestamp']}"
                        )
                        self.producer.send(
                        topic='globalmart.cart_events',
                        key=cart_event['cart_id'],
                        value=cart_event,
                        timestamp_ms=int(time.time() * 1000)
                        )
                        if random.random() < self.purchase_probability:
                            purchased = random.sample(
                                cart_event["products"],
                                random.randint(1, len(cart_event["products"]))
                            )
                            transaction_event = self.generate_transaction_event(
                                user_data["user_id"], purchased
                            )
                            print(
                            f"Transaction Event: {transaction_event['transaction_id']}- "
                            f"{transaction_event['user_id']} - {transaction_event['products']} - "
                            f"{transaction_event['total_amount']} - {transaction_event['payment_method']} - "
                            f"{transaction_event['timestamp']}"
                            )
                            self.producer.send(
                            topic='globalmart.transaction_events',
                            key=transaction_event['transaction_id'],
                            value=transaction_event,
                            timestamp_ms=int(time.time() * 1000)
                            )
                
                time.sleep(interval + random.uniform(-0.5, 0.5))
            except Exception as e:
                print(f"Error producing data: {e}")
                time.sleep(5)


    # def start_simulation(self, num_sensors=10):
    #     """Start multi-threaded sensor simulation"""
    #     print(f"🚀 Starting IoT simulation with {num_sensors} sensors...")
        
    #     # Create topic if not exists
    #     from kafka.admin import KafkaAdminClient, NewTopic
    #     admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    #     try:
    #         topic = NewTopic(name='iot-sensors', num_partitions=3, replication_factor=1)
    #         admin.create_topics([topic])
    #         print("✅ Created topic 'iot-sensors'")
    #     except:
    #         print("ℹ️ Topic 'iot-sensors' already exists")
        
    #     # Start producer threads
    #     threads = []
    #     for i in range(num_sensors):
    #         thread = threading.Thread(
    #             target=self.produce_sensor_stream,
    #             args=(i, random.uniform(0.5, 2))
    #             #args=(i, random.uniform(5, 10))
    #         )
    #         thread.start()
    #         threads.append(thread)
        
    #     try:
    #         # Run for specified duration
    #         time.sleep(60)  # Run for 1 minute
    #     except KeyboardInterrupt:
    #         print("\n⏹️ Stopping simulation...")
    #     finally:
    #         self.running = False
    #         for thread in threads:
    #             thread.join()
    #         self.producer.close()
    #         print("✅ Simulation stopped")

# Run the producer
if __name__ == "__main__":
    config = {
        'countries': ['USA', 'Canada', 'UK', 'Germany', 'France'],
        'start_date': '2020-01-01',
        'purchase_probability': 0.116,
        'cart_probability': 0.02,
        'events_per_second': 500,
        'user_error_rate': 0.01,
        'product_view_error_rate': 0.005,
        'cart_event_error_rate': 0.002,
        'transaction_event_error_rate': 0.001,
        'product_error_rate': 0.001
    }
    producer = GlobalMartProducer()
    #producer.start_simulation(num_sensors=5)