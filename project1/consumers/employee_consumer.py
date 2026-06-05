from kafka import KafkaConsumer
import json
import psycopg2

# Configuration
KAFKA_TOPIC = "employee-salaries"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "postgres"
}

def process_messages():
    # Connect to Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("🔄 Listening for messages... Press Ctrl+C to stop\n")
    message_count = 0
    
    try:
        for message in consumer:
            e = message.value  # e = employee data
            message_count += 1
            
            print(f"📨 Message {message_count}: {e['department']} | {e['position_title']} | ${e['salary']}")
            
            # Insert into department_employee table
            cursor.execute("""
                INSERT INTO department_employee 
                (department, department_division, position_title, hire_date, salary)
                VALUES (%s, %s, %s, %s, %s)
            """, (e['department'], e['department_division'], e['position_title'], e['hire_date'], e['salary']))
            
            # Update department salary totals (using assignment's helper code)
            cursor.execute("""
                INSERT INTO department_employee_salary (department, total_salary)
                VALUES (%s, %s) 
                ON CONFLICT(department) DO UPDATE 
                SET total_salary = department_employee_salary.total_salary + %s
            """, (e['department'], e['salary'], e['salary']))
            
            conn.commit()
            print(f"   ✅ Stored in database\n")
    
    except KeyboardInterrupt:
        print(f"\n⏹️ Consumer stopped after processing {message_count} messages")
        
        # Display final department totals
        cursor.execute("SELECT department, total_salary FROM department_employee_salary ORDER BY total_salary DESC")
        results = cursor.fetchall()
        
        print("\n" + "="*50)
        print("📊 DEPARTMENT SALARY TOTALS")
        print("="*50)
        for dept, total in results:
            print(f"   {dept}: ${total:,.2f}")
        print("="*50)
    
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        print("\n🔌 Connections closed")

if __name__ == "__main__":
    print("🚀 Starting Consumer...\n")
    process_messages()