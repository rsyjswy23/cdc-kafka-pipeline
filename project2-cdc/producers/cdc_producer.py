import json
import psycopg2
from kafka import KafkaProducer
import os
import sys

KAFKA_TOPIC = "cdc-employees"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
DLQ_TOPIC = "cdc-dead-letter-queue"
OFFSET_FILE = 'producer_offset.txt'

SOURCE_DB = {
    "host": "localhost",
    "port": 5433,
    "database": "sourcedb",
    "user": "postgres",
    "password": "postgres"
}

def get_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

def send_to_dlq(producer, error, record):
    producer.send(DLQ_TOPIC, value={'error': str(error), 'record': record})

def process_cdc_once():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    conn = psycopg2.connect(**SOURCE_DB)
    cursor = conn.cursor()
    
    last_offset = get_offset()
    print(f"Starting from offset: {last_offset}")
    
    cursor.execute("""
        SELECT cdc_id, emp_id, first_name, last_name, dob, city, salary, action
        FROM emp_cdc WHERE cdc_id > %s ORDER BY cdc_id ASC
    """, (last_offset,))
    
    rows = cursor.fetchall()
    
    for row in rows:
        cdc_id, emp_id, first_name, last_name, dob, city, salary, action = row
        
        message = {
            'cdc_id': cdc_id, 'emp_id': emp_id, 'first_name': first_name,
            'last_name': last_name, 'dob': str(dob), 'city': city,
            'salary': salary, 'action': action
        }
        
        try:
            producer.send(KAFKA_TOPIC, value=message)
            producer.flush()
            print(f"Sent: {action} | emp_id={emp_id}")
            save_offset(cdc_id)
        except Exception as e:
            send_to_dlq(producer, e, message)
            print(f"Failed: {e}")
    
    producer.close()
    cursor.close()
    conn.close()
    print(f"Processed {len(rows)} records\n")

if __name__ == "__main__":
    if '--once' in sys.argv:
        process_cdc_once()
    else:
        while True:
            process_cdc_once()
            import time
            time.sleep(2)