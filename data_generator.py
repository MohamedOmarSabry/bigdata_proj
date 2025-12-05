from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime, timedelta
import threading
from kafka.admin import KafkaAdminClient, NewTopic
class GlobalMartProducer:
    def __init__(self, bootstrap_servers='localhost:9092',config={}):
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
        self.system_error_rate=config.get('system_error_rate',0.001)
        self.running = True
    def failure_injector(self,record):
        if random.random() < self.system_error_rate:
            safe_faults = ['email', 'age', 'country', 'registration_date', 'total_amount', 'payment_method']
            available_faults = [f for f in safe_faults if f in record]
            if available_faults:
                field_to_fault = random.choice(available_faults)
                fault_type = random.choice(['negative', 'missing_field', 'invalid_value'])
                value = record[field_to_fault]
                if fault_type == 'negative':
                    if isinstance(value, (int, float)):
                        record[field_to_fault]= -abs(value)
                    elif isinstance(value, str):
                        record[field_to_fault]= "<corrupted>"
                elif fault_type == 'missing_field':
                    record[field_to_fault]= None
                elif fault_type == 'invalid_value':
                    if isinstance(value, (int, float)):
                        record[field_to_fault]= "INVALID_VALUE"
                    elif isinstance(value, str):
                        record[field_to_fault]= 999999
            return record
        else:
            return record
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
        record= {
            "user_id":user_id,
            "email": email,
            "age": age,
            "country": country,
            "registration_date": registration_date.isoformat(),
            "preferences": preferences,
        }
        return self.failure_injector(record)
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
        cuid = str(uuid.uuid4())
        cart_id = f"cart_{cuid}"
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
        tuid = str(uuid.uuid4())
        transaction_id = f"transaction_{tuid}"
        random.seed(transaction_id)
        timestamp = datetime.now().isoformat()
        total_amount=0
        for p in products:
            pid = p["product_id"]
            qty = p["quantity"]
            prc = p["price"]
            self.products[pid]["inventory"] -= qty
            total_amount += qty * prc
        record= {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "timestamp": timestamp,
            "products": products,
            "total_amount": round(total_amount,2),
            "payment_method": random.choice(["credit_card", "paypal", "gift_card"]),
        }
        return self.failure_injector(record)

    #Idea generate user --> generate products --> generate product view --> generate cart event --> generate transaction event
    def produce_data_stream(self, interval=1):
        """Continuously produce sensor data"""
        interval = 1 / self.events_per_sec
        while self.running:
            try:
                user_data = self.generate_user()
                print(
                    f"User: {user_data['user_id']} - "
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
                    product_id = product_view_event['product_id']
                    product_payload = self.products[product_id].copy()
                    product_payload["product_id"] = product_id
                    product_data_L.append(product_view_event['product_id'])
                    print(
                    f"Product View: {product_view_event['event_id']} - "
                    f"{product_view_event['product_id']} - {product_view_event['user_id']} - "
                    f"{product_view_event['timestamp']}"
                    )
                    print(
                    f"Product: {product_payload['product_id']} - "
                    f"{product_payload['price']} - {product_payload['inventory']} - "
                    f"{product_payload['category']}"
                    )
                    self.producer.send(
                        topic='globalmart.product_views',
                        key=product_view_event['event_id'],
                        value=product_view_event,
                        timestamp_ms=int(time.time() * 1000)
                    )
                    self.producer.send(
                        topic='globalmart.product_catalog',
                        key=product_id,
                        value=product_payload,
                        timestamp_ms=int(time.time() * 1000)
                    )
                    #Stream product
                if random.random() < self.cart_probability:
                    cart_event = self.generate_cart_event(product_data_L, user_data["user_id"])
                    if cart_event and len(cart_event["products"]) > 0:
                        print(
                        f"Cart Event: {cart_event['cart_id']} - "
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
                            transaction_event = self.generate_transcation_event(
                                user_data["user_id"], purchased
                            )
                            print(
                            f"Transaction Event: {transaction_event['transaction_id']} - "
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
                jitter = random.uniform(-0.1 * interval, 0.1* interval)
                time.sleep(max(0, interval + jitter))
            except Exception as e:
                print(f"Error producing data: {e}")
                time.sleep(5)

    
    def start_simulation(self, num_threads=5, run_seconds=60):
        """Start multi-threaded GlobalMart event simulation"""
        print(f"Starting GlobalMart data simulation with {num_threads} producer threads...")
        self.avg_events_per_loop = 23
        self.events_per_sec = self.events_per_sec /(self.avg_events_per_loop * num_threads)
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        topics = [
            "globalmart.users",
            "globalmart.product_views",
            "globalmart.cart_events",
            "globalmart.transaction_events",
            "globalmart.product_catalog"
        ]
        new_topics = []
        for t in topics:
            new_topics.append(NewTopic(name=t, num_partitions=3, replication_factor=1))
        try:
            admin.create_topics(new_topics)
            print("Kafka topics created:")
            for t in topics:
                print(f"    {t}")
        except:
            print("Kafka topics already exist")
        threads = []
        self.running = True
        for i in range(num_threads):
            thread = threading.Thread(
                target=self.produce_data_stream,
                args=(1,),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        print(f"Started {num_threads} producer threads")
        try:
            if run_seconds is None:
                while True:
                    time.sleep(1)
            else:
                time.sleep(run_seconds)
        except KeyboardInterrupt:
            print("\nStopping simulation...")
        finally:
            print("\nShutting down threads...")
            self.running = False

            for thread in threads:
                thread.join()

            self.producer.close()
            print("GlobalMart simulation stopped")


# Run the producer
if __name__ == "__main__":
    config = {
        'countries': ['USA', 'Canada', 'UK', 'Germany', 'France'],
        'start_date': '2020-01-01',
        'purchase_probability': 0.14,
        'cart_probability': 0.3,
        'events_per_second': 550, #target throughput
        'system_error_rate': 0.01,
    }
    producer = GlobalMartProducer(config=config)
    producer.start_simulation(num_threads=5, run_seconds=120)