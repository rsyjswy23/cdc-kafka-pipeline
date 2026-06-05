import pandas as pd
import json
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = "employee-salaries"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
CSV_PATH = '/Users/hui/Documents/kafka-test/data/Employee_Salaries.csv'  # Relative path - works from producers folder

# Only these departments as per assignment
ALLOWED_DEPARTMENTS = ['ECC', 'CIT', 'EMS']

def process_csv_and_send():
    # Connect to Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Read CSV
    df = pd.read_csv(CSV_PATH)
    print(f"📖 Read {len(df)} records from CSV\n")
    
    sent_count = 0
    
    # Process each row
    for _, row in df.iterrows():
        department = row['Department']
        
        # Filter 1: Only ECC, CIT, EMS
        if department not in ALLOWED_DEPARTMENTS:
            continue
            
        # Filter 2: Hired after 2010
        hire_year = pd.to_datetime(row['Initial Hire Date'], format='%d-%b-%Y').year
        if hire_year < 2010:
            continue
        
        # Transform: Round salary down (floor)
        salary = int(row['Salary'])  # int() floors positive numbers
        
        # Create message
        message = {
            'department': department,
            'department_division': row['Department Division'],
            'position_title': row['Position Title'],
            'hire_date': pd.to_datetime(row['Initial Hire Date'], format='%d-%b-%Y').strftime('%Y-%m-%d'),
            'salary': salary
        }
        
        # Send to Kafka
        producer.send(KAFKA_TOPIC, value=message)
        sent_count += 1
        print(f"✅ Sent: {department} | {message['position_title']} | Salary: ${salary}")
    
    # Clean up
    producer.flush()
    producer.close()
    
    print(f"\n📊 Summary: Sent {sent_count} records to Kafka")

if __name__ == "__main__":
    print("🚀 Starting Producer...\n")
    process_csv_and_send()
    print("\n✨ Done!")