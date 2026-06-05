from kafka import KafkaConsumer
import json
import psycopg2

KAFKA_TOPIC = "cdc-employees"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

DEST_DB = {
    "host": "localhost",
    "port": 5434,
    "database": "destdb",
    "user": "postgres",
    "password": "postgres"
}

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id='cdc-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = psycopg2.connect(**DEST_DB)
cursor = conn.cursor()

print("Listening for CDC messages...")

for msg in consumer:
    e = msg.value
    print(f"{e['action']}: emp_id={e['emp_id']}")
    
    try:
        if e['action'] == 'INSERT':
            cursor.execute("""
                INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (e['emp_id'], e['first_name'], e['last_name'], e['dob'], e['city'], e['salary']))
        elif e['action'] == 'UPDATE':
            cursor.execute("""
                UPDATE employees SET first_name=%s, last_name=%s, dob=%s, city=%s, salary=%s
                WHERE emp_id=%s
            """, (e['first_name'], e['last_name'], e['dob'], e['city'], e['salary'], e['emp_id']))
        elif e['action'] == 'DELETE':
            cursor.execute("DELETE FROM employees WHERE emp_id=%s", (e['emp_id'],))
        
        conn.commit()
        print(f"Synced to destination")
    except Exception as err:
        print(f"Error: {err}")
        conn.rollback()