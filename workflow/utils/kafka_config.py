from confluent_kafka import Producer
import json

# Create a single global producer instance for reuse
_PRODUCER_INSTANCE = Producer({
    "bootstrap.servers": "kafka:9092",
    "queue.buffering.max.messages": 100000 # Good practice for high throughput
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def publish_message(topic, message):
    # Use the global instance without recreating connections
    _PRODUCER_INSTANCE.produce(
        topic=topic,
        value=json.dumps(message, default=str),
        callback=delivery_report
    )

def flush_kafka_messages():
    # Call this at the very end of the DAG task execution
    _PRODUCER_INSTANCE.flush()