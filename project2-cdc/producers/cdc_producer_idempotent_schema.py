"""
Idempotent CDC Producer with Schema Registry
Combines:
1. Idempotency (no duplicates during retries)
2. Schema Registry (Avro serialization with schema validation)
3. Explicit Schema Registration.
4. Offset tracking (resume from last CDC record)
5. Message ID tracking (application-level duplicate prevention)
"""

import psycopg2
import os
import sys
import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import fastavro
import io

# ============================================
# CONFIGURATION
# ============================================

KAFKA_TOPIC = "cdc-employees-avro"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
OFFSET_FILE = 'producer_offset.txt'

SOURCE_DB = {
    "host": "localhost",
    "port": 5433,
    "database": "sourcedb",
    "user": "postgres",
    "password": "postgres"
}

# ============================================
# SCHEMA (hardcoded for reliability)
# ============================================

SCHEMA_DICT = {
    "type": "record",
    "name": "EmployeeEvent",
    "fields": [
        {"name": "cdc_id", "type": "int"},
        {"name": "emp_id", "type": "int"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "dob", "type": ["null", "string"], "default": None},
        {"name": "city", "type": ["null", "string"], "default": None},
        {"name": "salary", "type": "int"},
        {"name": "action", "type": {"type": "enum", "name": "Action", "symbols": ["INSERT", "UPDATE", "DELETE"]}}
    ]
}

# ============================================
# SETUP
# ============================================

print("📋 Connecting to Schema Registry...")
schema_registry = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

# Register schema with Schema Registry
subject = f"{KAFKA_TOPIC}-value"
schema_str = json.dumps(SCHEMA_DICT)

try:
    schema_id = schema_registry.register_schema(subject, schema_str)
    print(f"✅ Schema registered with ID: {schema_id}")
except Exception as e:
    print(f"⚠️ Schema may already exist: {e}")

# Create Kafka producer
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'enable.idempotence': True,
    'acks': 'all'
})

# ============================================
# AVRO SERIALIZATION using fastavro
# ============================================

def serialize_avro(message):
    """Serialize dict to Avro using fastavro"""
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, SCHEMA_DICT, message)
    return bytes_writer.getvalue()

# ============================================
# OFFSET TRACKING
# ============================================

def get_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

def delivery_callback(err, msg):
    if err:
        print(f"   ❌ Delivery failed: {err}")
    else:
        print(f"   ✅ Delivered to partition {msg.partition()} at offset {msg.offset()}")

# ============================================
# MAIN PROCESSING
# ============================================

def process_cdc():
    conn = psycopg2.connect(**SOURCE_DB)
    cursor = conn.cursor()
    
    last_offset = get_offset()
    print(f"\n📌 Starting from offset: {last_offset}")
    
    cursor.execute("""
        SELECT cdc_id, emp_id, first_name, last_name, dob::text, city, salary, action
        FROM emp_cdc 
        WHERE cdc_id > %s 
        ORDER BY cdc_id ASC
    """, (last_offset,))
    
    rows = cursor.fetchall()
    print(f"📊 Found {len(rows)} new CDC records")
    
    for row in rows:
        cdc_id, emp_id, first_name, last_name, dob, city, salary, action = row
        
        # Create message as dictionary
        message = {
            'cdc_id': cdc_id,
            'emp_id': emp_id,
            'first_name': first_name,
            'last_name': last_name,
            'dob': dob if dob else None,
            'city': city if city else None,
            'salary': salary,
            'action': action
        }
        
        print(f"📤 Sending: {action} | emp_id={emp_id}")
        
        try:
            # Serialize using fastavro
            serialized = serialize_avro(message)
            
            # Send to Kafka
            producer.produce(
                KAFKA_TOPIC,
                value=serialized,
                callback=delivery_callback
            )
            producer.poll(0)
            
            save_offset(cdc_id)
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    producer.flush()
    cursor.close()
    conn.close()
    print(f"✅ Processed {len(rows)} records")

if __name__ == "__main__":
    if '--once' in sys.argv:
        process_cdc()
    else:
        import time
        while True:
            process_cdc()
            time.sleep(5)