import io
import avro.schema
from avro.io import DatumReader, BinaryEncoder
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer

# Define Avro schema
schema_str = """
{
  "namespace": "advertise.example",
  "type": "record",
  "name": "BidRequest",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "device", "type": "string"},
    {"name": "bid_floor", "type": "float"},
    {"name": "ad_position", "type": "string"},
    {"name": "interests", "type": {"type": "array", "items": "string"}},
    {"name": "age_range", "type": "string"}
  ]
}
"""

# Parse Avro schema
schema = avro.schema.parse(schema_str)

# Create Kafka producer
producer = AvroProducer({
    'bootstrap.servers': 'kafka-broker-1:9092,kafka-broker-2:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_key_schema=None, default_value_schema=schema)

# Sample bid request data
bid_request = {
    "user_id": "user_456",
    "location": "New York",
    "device": "mobile",
    "bid_floor": 0.5,
    "ad_position": "banner",
    "interests": ["sports", "travel"],
    "age_range": "25-35"
}

# Encode Avro message
writer = io.BytesIO()
encoder = BinaryEncoder(writer)
datum_writer = DatumReader(schema)
datum_writer.write(bid_request, encoder)
avro_message = writer.getvalue()

# Produce message to Kafka topic
producer.produce(topic='bid_requests', value=avro_message)
producer.flush()


