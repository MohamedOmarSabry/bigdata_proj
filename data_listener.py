from kafka import KafkaConsumer
import json
import time
import os

TOPICS = [
    "globalmart.users",
    "globalmart.product_views",
    "globalmart.cart_events",
    "globalmart.transaction_events",
    "globalmart.product_catalog",
    "globalmart.faulty_data"
]

def clear():
    os.system("cls" if os.name == "nt" else "clear")

def main():
    print("Starting GlobalMart Monitoring Consumer...")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="globalmart-monitor",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None
    )

    # Stats
    total_events = 0
    topic_counts = {t: 0 for t in TOPICS}
    last_messages = {t: None for t in TOPICS}
    start_time = time.time()
    events_since_last = 0
    last_update = start_time
    
    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=500)

            now = time.time()

            # Process all messages
            for tp, messages in msg_pack.items():
                for msg in messages:
                    topic = msg.topic
                    topic_counts[topic] += 1
                    total_events += 1
                    events_since_last += 1
                    last_messages[topic] = msg.value

            # Update display every 2 seconds
            if now - last_update >= 2:
                elapsed = now - start_time
                interval = now - last_update
                eps = events_since_last / interval if interval > 0 else 0

                clear()
                print("GLOBALMART EVENT MONITOR")
                print("────────────────────────────\n")

                print(f"Uptime: {elapsed:.1f} sec")
                print(f"Throughput: {eps:.1f} events/sec")
                print(f"Total events: {total_events}\n")

                print("Per-topic counts:")
                for t in TOPICS:
                    print(f"  • {t}: {topic_counts[t]}")

                print("\nLatest sample events:")
                for t in TOPICS:
                    print(f"\n=== {t} ===")
                    if last_messages[t] is None:
                        print("  (no messages yet)")
                    else:
                        pretty = json.dumps(last_messages[t], indent=4)
                        for line in pretty.split("\n"):
                            print("  " + line)
                events_since_last = 0
                last_update = now

    except KeyboardInterrupt:
        print("\nStopping monitor...")

    finally:
        consumer.close()
        print("Consumer closed")


if __name__ == "__main__":
    main()
