import json
from confluent_kafka import Producer

# Create Kafka producer
producer = Producer({'bootstrap.servers': 'kafka-broker-1:9092,kafka-broker-2:9092'})

# Sample ad impression data
ad_impression = {
    "ad_creative_id": "ad_123",
    "user_id": "user_456",
    "timestamp": "2023-04-30T10:15:30Z",
    "website": "example.com"
}

# Produce message to Kafka topic
producer.produce('ad_impressions', json.dumps(ad_impression).encode('utf-8'))
producer.flush()