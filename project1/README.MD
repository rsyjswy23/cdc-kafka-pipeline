



## 1. Start Docker Containers
```
docker-compose up -d
sleep 15  # Wait for containers to initialize
docker ps  # Check all containers running
```

```
# Install Confluent Kafka libraries
pip install confluent-kafka avro-python3 requests
```

## 2. Initialize Databases (First time only)
create 3 tables in sourcedb (5433) AND destdb (5434).

## 3. Create Kafka Topics
```
# Delete existing topic (if any)
docker exec $(docker ps -qf "name=kafka") kafka-topics \
  --bootstrap-server kafka:9092 \
  --delete --topic cdc-employees || true

# Create main CDC topic with 3 partitions
docker exec $(docker ps -qf "name=kafka") kafka-topics --bootstrap-server kafka:9092 \
  --create --topic cdc-employees --partitions 3 --replication-factor 1

# Create DLQ topic
docker exec $(docker ps -qf "name=kafka") kafka-topics --bootstrap-server kafka:9092 \
  --create --topic cdc-dead-letter-queue --partitions 1 --replication-factor 1

# Create Avro topic for Schema Registry
docker exec $(docker ps -qf "name=kafka") kafka-topics --bootstrap-server kafka:9092 \
  --create --topic cdc-employees-avro \
  --partitions 3 \
  --replication-factor 1

# Verify topics were created
docker exec $(docker ps -qf "name=kafka") kafka-topics --bootstrap-server kafka:9092 --list
```

## 4. Consumer Group (3)
```
source venv/bin/activate 
python consumers/cdc_consumer.py
python consumers/cdc_consumer_schema.py
```

## 5. Monitor Consumer Group 
```
# Check consumer group status
docker exec $(docker ps -qf "name=kafka") kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group cdc-group \
  --describe

# Check current consumer group status
docker exec $(docker ps -qf "name=kafka") kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group fastavro-consumer-group \
  --describe
```

## 6.Producer
```
# Reset offset first
rm -f processed_ids.txt producer_offset.txt

rm -f producer_offset.txt

# Run producer ONCE to check for existing records
python producers/cdc_producer.py

# Test idempotent producer
python producers/cdc_producer_idempotent.py
```

## 7. Insert/Update/Delete Data (in DBeaver on sourcedb)

## 8. Test DLQ
```
python test_dlq.py

# Read from DLQ
docker exec $(docker ps -qf "name=kafka") kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic cdc-dead-letter-queue \
  --from-beginning \
  --max-messages 5

```

## 9. Monitor Everything
```
# Check consumer group assignment
docker exec $(docker ps -qf "name=kafka") kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group cdc-group \
  --describe

# Check Kafka topic details
docker exec $(docker ps -qf "name=kafka") kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe --topic cdc-employees


# Run once
./check_consumers.sh

# Real-time monitoring (refreshes every 2 seconds)
./check_consumers.sh watch

# clean up
./cleanup_all.sh
```


report output: 


By adding Schema Registry, it ensures that producers and consumers agree on data formats and prevents schema drift.

Highlight in this project:
Benefits of Combining Both
Idempotency = No duplicates during network retries

Schema Registry = Data validation + forward/backward compatibility

Message ID tracking = Application-level duplicate prevention


╔══════════════════════════════════════════════════════════════════════════════╗
║                    📊 CDC PIPELINE COMPREHENSIVE STATUS                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

📌 CONSUMER GROUPS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Group: cdc-group
    Partition TOPIC: Offset=CURRENT-OFFSET | Lag=LAG | ❌ BACKLOG
    Partition cdc-employees: Offset=0 | Lag=0 | ✅ CAUGHT UP

  Group: schema-consumer-group
    Partition TOPIC: Offset=CURRENT-OFFSET | Lag=LAG | ❌ BACKLOG
    Partition cdc-employees-avro: Offset=1 | Lag=0 | ✅ CAUGHT UP
    Partition cdc-employees-avro: Offset=4 | Lag=0 | ✅ CAUGHT UP
    Partition cdc-employees-avro: Offset=1 | Lag=0 | ✅ CAUGHT UP

  Group: fastavro-consumer-group
    ⚠️  Consumer group not active (no consumers running)

📌 SCHEMA REGISTRY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✅ Schema Registry running at http://localhost:8081
  📋 Registered subjects: ["cdc-employees-avro-value"]
  📄 Avro schema ID: 1 (version 1)

📌 SCHEMA REGISTRY ERRORS (Why messages FAIL to write to Destination DB)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✅ No schema validation errors detected
     All CDC records passed Avro validation and were sent to Kafka

📌 DEAD LETTER QUEUE (Messages that failed Kafka processing)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ❌ 6 messages in Dead Letter Queue

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                    DETAILED DLQ MESSAGE ANALYSIS                            │
  └─────────────────────────────────────────────────────────────────────────────┘

  📋 SUMMARY: These messages were sent to Kafka but FAILED to be processed by consumers
              They are stored in DLQ for manual inspection and reprocessing.

📌 PRODUCER OFFSET STATUS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  📍 Last processed CDC record: 8
  ⏳ -1 records pending to send (CDC table has new records)

📌 END-TO-END SYNC VERIFICATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Source DB (sourcedb): 4 employees
  Destination DB (destdb): 6 employees
  ⚠️  Difference: -2 records (records that failed validation or are pending)

  Possible reasons for mismatch:
    • Records failed Avro schema validation (see SCHEMA REGISTRY ERRORS above)
    • Records are in Dead Letter Queue (see DLQ section above)
    • Producer hasn't processed pending CDC records

╔══════════════════════════════════════════════════════════════════════════════╗
║                           SUMMARY OF FAILURES                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
  ✅ Schema Validation: No failures
  ❌ Dead Letter Queue: 6 messages failed
     → These messages reached Kafka but failed consumer processing
  ⚠️  Sync Mismatch: -2 records difference between source and destination

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  PIPELINE STATUS: DEGRADED - 2 issue(s) detected
   Review sections above for details and fix recommendations
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━