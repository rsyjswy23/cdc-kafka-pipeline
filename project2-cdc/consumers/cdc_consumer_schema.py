"""
CDC Consumer with Schema Registry - Using fastavro
"""

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
import psycopg2
import fastavro
import io
import json

# ============================================
# CONFIGURATION
# ============================================

KAFKA_TOPIC = "cdc-employees-avro"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

DEST_DB = {
    "host": "localhost",
    "port": 5434,
    "database": "destdb",
    "user": "postgres",
    "password": "postgres"
}

# ============================================
# SCHEMA REGISTRY SETUP
# ============================================

print("🔄 Connecting to Schema Registry...")

# Connect to schema registry
client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

# Get the schema
subject = f"{KAFKA_TOPIC}-value"
registered_schema = client.get_latest_version(subject)
schema_str = registered_schema.schema.schema_str

# Parse the schema for fastavro
schema = fastavro.schema.parse_schema(json.loads(schema_str))
print(f"✅ Schema loaded: {subject} v{registered_schema.version}")

# ============================================
# KAFKA CONSUMER
# ============================================

consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'schema-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
})

consumer.subscribe([KAFKA_TOPIC])
print(f"📌 Subscribed to topic: {KAFKA_TOPIC}")
print()

# ============================================
# DATABASE SETUP
# ============================================

conn = psycopg2.connect(**DEST_DB)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        emp_id INTEGER PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        dob DATE,
        city VARCHAR(100),
        salary INTEGER
    )
""")
conn.commit()

print("🔄 Listening for Avro messages...")
print("=" * 50)

# ============================================
# MESSAGE PROCESSING
# ============================================

def decode_avro_message(data):
    """Decode Avro message using fastavro"""
    bytes_reader = io.BytesIO(data)
    return fastavro.schemaless_reader(bytes_reader, schema)

message_count = 0

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue
        
        try:
            # Decode Avro message
            value = decode_avro_message(msg.value())
            message_count += 1
            
            action = value['action']
            print(f"\n📨 [{message_count}] {action}: emp_id={value['emp_id']}")
            print(f"   Name: {value['first_name']} {value['last_name']}")
            print(f"   Salary: {value['salary']}")
            
            # Apply to database
            if action == 'INSERT':
                cursor.execute("""
                    INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (emp_id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        dob = EXCLUDED.dob,
                        city = EXCLUDED.city,
                        salary = EXCLUDED.salary
                """, (value['emp_id'], value['first_name'], value['last_name'],
                      value['dob'], value['city'], value['salary']))
                
            elif action == 'UPDATE':
                cursor.execute("""
                    UPDATE employees 
                    SET first_name=%s, last_name=%s, dob=%s, city=%s, salary=%s
                    WHERE emp_id=%s
                """, (value['first_name'], value['last_name'], value['dob'],
                      value['city'], value['salary'], value['emp_id']))
                
            elif action == 'DELETE':
                cursor.execute("DELETE FROM employees WHERE emp_id=%s", (value['emp_id'],))
            
            conn.commit()
            print(f"   ✅ Synced to database")
            
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            conn.rollback()

except KeyboardInterrupt:
    print(f"\n\n⏹️ Stopped after {message_count} messages")

finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("🔌 Connections closed")