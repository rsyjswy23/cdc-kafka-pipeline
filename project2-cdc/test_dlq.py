from kafka import KafkaProducer
import json
import sys

# Add path for imports
sys.path.append('/Users/hui/Documents/kafka-test/project2-cdc')

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Sending test messages to DLQ...")

# Send malformed messages (missing required fields)
bad_messages = [
    {'emp_id': 999, 'error': 'Missing action field'},
    {'action': 'INSERT', 'error': 'Missing emp_id'},
    {'bad_data': 'Completely malformed', 'error': True},
]

for i, msg in enumerate(bad_messages):
    producer.send('cdc-dead-letter-queue', value=msg)
    print(f"Sent bad message {i+1}: {msg}")

producer.flush()
producer.close()
print("\n All test messages sent to Dead Letter Queue!")
print("Check DLQ: docker exec ... kafka-console-consumer --topic cdc-dead-letter-queue")