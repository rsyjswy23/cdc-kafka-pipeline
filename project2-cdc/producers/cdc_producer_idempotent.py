import pandas as pd
import json
from kafka import KafkaProducer
import os
import sys
import hashlib

KAFKA_TOPIC = "cdc-employees"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
OFFSET_FILE = 'producer_offset.txt'
PROCESSED_IDS_FILE = 'processed_ids.txt'  # Track processed message IDs

SOURCE_DB = {
    "host": "localhost",
    "port": 5433,
    "database": "sourcedb",
    "user": "postgres",
    "password": "postgres"
}

def get_processed_ids():
    """Load already processed message IDs"""
    if os.path.exists(PROCESSED_IDS_FILE):
        with open(PROCESSED_IDS_FILE, 'r') as f:
            return set(line.strip() for line in f)
    return set()

def save_processed_id(msg_id):
    """Save processed message ID"""
    with open(PROCESSED_IDS_FILE, 'a') as f:
        f.write(f"{msg_id}\n")

def get_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

def generate_message_id(record):
    """Generate unique ID for each message to prevent duplicates"""
    unique_string = f"{record['cdc_id']}_{record['emp_id']}_{record['action']}"
    return hashlib.md5(unique_string.encode()).hexdigest()

def process_cdc_once():
    """Process CDC records with idempotency"""
    
    # Create idempotent producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        enable_idempotence=True,  # <-- KEY: Prevents duplicates
        acks='all',                # <-- Wait for all replicas
        retries=3,                 # <-- Retry on failure
        max_in_flight_requests_per_connection=1  # <-- Preserve ordering
    )
    
    # Connect to database
    import psycopg2
    conn = psycopg2.connect(**SOURCE_DB)
    cursor = conn.cursor()
    
    last_offset = get_offset()
    processed_ids = get_processed_ids()
    
    print(f"Starting from offset: {last_offset}")
    print(f"Already processed: {len(processed_ids)} messages")
    
    cursor.execute("""
        SELECT cdc_id, emp_id, first_name, last_name, dob, city, salary, action
        FROM emp_cdc WHERE cdc_id > %s ORDER BY cdc_id ASC
    """, (last_offset,))
    
    rows = cursor.fetchall()
    new_messages = 0
    duplicate_skipped = 0
    
    for row in rows:
        cdc_id, emp_id, first_name, last_name, dob, city, salary, action = row
        
        message = {
            'cdc_id': cdc_id,
            'emp_id': emp_id,
            'first_name': first_name,
            'last_name': last_name,
            'dob': str(dob),
            'city': city,
            'salary': salary,
            'action': action
        }
        
        # Generate unique ID for this message
        # but not enough for rows with same values. 
        # ('Alice', 'Abe', '1988-03-25', 'Houston', 70000),
        # ('Alice', 'Abe', '1988-03-25', 'Houston', 70000);
        msg_id = generate_message_id(message)
        
        # Check if already processed (idempotency check)
        if msg_id in processed_ids:
            print(f"Skipping duplicate: {action} emp_id={emp_id} (already sent)")
            duplicate_skipped += 1
            continue
        
        try:
            # Send to Kafka with idempotent producer
            future = producer.send(KAFKA_TOPIC, value=message)
            result = future.get(timeout=10)  # Wait for confirmation
            
            print(f"Sent: {action} | emp_id={emp_id} | Partition={result.partition} | Offset={result.offset}")
            
            # Record as processed
            save_processed_id(msg_id)
            save_offset(cdc_id)
            new_messages += 1
            
        except Exception as e:
            print(f"Failed to send: {e}")
            # Don't save offset or processed ID - will retry next time
    
    producer.flush()
    producer.close()
    cursor.close()
    conn.close()
    
    print(f"\n Summary:")
    print(f"   New messages sent: {new_messages}")
    print(f"   Duplicates skipped: {duplicate_skipped}")
    print(f"   Total processed: {len(processed_ids) + new_messages}")

if __name__ == "__main__":
    if '--once' in sys.argv:
        process_cdc_once()
    else:
        import time
        while True:
            process_cdc_once()
            time.sleep(5)