#!/bin/bash

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    📊 CDC PIPELINE COMPREHENSIVE STATUS                       ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

# ============================================
# 1. CONSUMER GROUP STATUS
# ============================================
echo "📌 CONSUMER GROUPS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for GROUP in "cdc-group" "schema-consumer-group" "fastavro-consumer-group"; do
    echo ""
    echo "  Group: $GROUP"
    
    GROUP_INFO=$(docker exec $(docker ps -qf "name=kafka") kafka-consumer-groups \
      --bootstrap-server kafka:9092 \
      --group $GROUP --describe 2>/dev/null)
    
    if [ -z "$GROUP_INFO" ] || echo "$GROUP_INFO" | grep -q "does not exist"; then
        echo "    ⚠️  Consumer group not active (no consumers running)"
    else
        echo "$GROUP_INFO" | awk 'NR>1 && NF>0 {
            if ($1 != "TOPIC" && $2 != "PARTITION") {
                partition=$2
                offset=$4
                lag=$6
                if (lag==0) status="✅ CAUGHT UP"
                else if (lag<10) status="⚠️  SLIGHT LAG"
                else status="❌ BACKLOG"
                printf "    Partition %s: Offset=%s | Lag=%s | %s\n", partition, offset, lag, status
            }
        }'
    fi
done

echo ""

# ============================================
# 2. SCHEMA REGISTRY STATUS
# ============================================
echo "📌 SCHEMA REGISTRY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
    echo "  ✅ Schema Registry running at http://localhost:8081"
    
    SUBJECTS=$(curl -s http://localhost:8081/subjects)
    echo "  📋 Registered subjects: $SUBJECTS"
    
    if echo "$SUBJECTS" | grep -q "cdc-employees-avro-value"; then
        SCHEMA_INFO=$(curl -s http://localhost:8081/subjects/cdc-employees-avro-value/versions/latest)
        SCHEMA_ID=$(echo "$SCHEMA_INFO" | python3 -c "import sys, json; print(json.load(sys.stdin).get('id', 'N/A'))" 2>/dev/null)
        SCHEMA_VERSION=$(echo "$SCHEMA_INFO" | python3 -c "import sys, json; print(json.load(sys.stdin).get('version', 'N/A'))" 2>/dev/null)
        echo "  📄 Avro schema ID: $SCHEMA_ID (version $SCHEMA_VERSION)"
    fi
else
    echo "  ❌ Schema Registry not reachable"
fi

echo ""

# ============================================
# 3. SCHEMA REGISTRY ERRORS (Why messages fail to write to dest_db)
# ============================================
echo "📌 SCHEMA REGISTRY ERRORS (Why messages FAIL to write to Destination DB)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SCHEMA_ERRORS=$(docker exec $(docker ps -qf "name=source_db") psql -U postgres -d sourcedb -t -c "
SELECT cdc_id, emp_id, first_name, action, salary,
       CASE 
           WHEN action NOT IN ('INSERT', 'UPDATE', 'DELETE') THEN 'Invalid action value - must be INSERT, UPDATE, or DELETE'
           WHEN first_name IS NULL THEN 'Missing required field: first_name cannot be NULL'
           WHEN first_name = '' THEN 'Missing required field: first_name cannot be empty string'
           WHEN salary IS NULL THEN 'Missing required field: salary cannot be NULL'
           WHEN salary < 0 THEN 'Invalid salary value - cannot be negative'
           WHEN salary = 0 THEN 'Warning: Zero salary - accepted but unusual'
           ELSE NULL
       END as error_reason,
       CASE 
           WHEN action NOT IN ('INSERT', 'UPDATE', 'DELETE') THEN '❌ REJECTED - Message never sent to Kafka'
           WHEN first_name IS NULL OR first_name = '' THEN '❌ REJECTED - Avro schema validation failed'
           WHEN salary IS NULL THEN '❌ REJECTED - Avro schema validation failed'
           ELSE '✅ ACCEPTED'
       END as status
FROM emp_cdc 
WHERE action NOT IN ('INSERT', 'UPDATE', 'DELETE')
   OR first_name IS NULL OR first_name = ''
   OR salary IS NULL OR salary < 0
ORDER BY cdc_id DESC 
LIMIT 10;" 2>/dev/null)

if [ -n "$SCHEMA_ERRORS" ]; then
    echo "  ⚠️  Records that failed schema validation (not written to destination):"
    echo ""
    
    echo "$SCHEMA_ERRORS" | while read line; do
        if [ -n "$line" ]; then
            cdc_id=$(echo "$line" | awk '{print $1}')
            emp_id=$(echo "$line" | awk '{print $2}')
            first_name=$(echo "$line" | awk '{print $3}')
            action=$(echo "$line" | awk '{print $4}')
            salary=$(echo "$line" | awk '{print $5}')
            error=$(echo "$line" | awk '{$1=$2=$3=$4=$5=""; print substr($0,6)}' | cut -d'|' -f1)
            status=$(echo "$line" | awk -F'|' '{print $NF}')
            
            echo "  ┌─────────────────────────────────────────────────────────────────────────────┐"
            echo "  │ 📨 CDC ID: $cdc_id | Employee ID: $emp_id"
            echo "  │    Action: $action | Name: ${first_name:-NULL} | Salary: ${salary:-NULL}"
            echo "  │    ❌ Error: $error"
            echo "  │    📍 Result: $status"
            
            # Additional explanation
            if echo "$error" | grep -q "Invalid action"; then
                echo "  │    💡 Fix: Update emp_cdc action to 'INSERT', 'UPDATE', or 'DELETE'"
            elif echo "$error" | grep -q "Missing required field: first_name"; then
                echo "  │    💡 Fix: Add a NOT NULL constraint to employees.first_name or provide default value"
            elif echo "$error" | grep -q "salary cannot be NULL"; then
                echo "  │    💡 Fix: Ensure salary has a value or set a default in Avro schema"
            elif echo "$error" | grep -q "negative"; then
                echo "  │    💡 Fix: Add check constraint to prevent negative salaries"
            fi
            echo "  └─────────────────────────────────────────────────────────────────────────────┘"
            echo ""
        fi
    done
    
    echo "  📋 SUMMARY: These records exist in source_db CDC table but were NEVER written to dest_db"
    echo "              because they failed Avro schema validation at the producer level."
else
    echo "  ✅ No schema validation errors detected"
    echo "     All CDC records passed Avro validation and were sent to Kafka"
fi

echo ""

# ============================================
# 4. DEAD LETTER QUEUE (Failed Kafka Messages)
# ============================================
echo "📌 DEAD LETTER QUEUE (Messages that failed Kafka processing)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

DLQ_COUNT=$(docker exec $(docker ps -qf "name=kafka") kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:9092 \
  --topic cdc-dead-letter-queue 2>/dev/null | \
  awk -F':' '{sum+=$3} END {print sum}')

if [ -z "$DLQ_COUNT" ] || [ "$DLQ_COUNT" = "0" ]; then
    echo "  ✅ No messages in DLQ - All Kafka messages processed successfully"
else
    echo "  ❌ $DLQ_COUNT messages in Dead Letter Queue"
    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "  │                    DETAILED DLQ MESSAGE ANALYSIS                            │"
    echo "  └─────────────────────────────────────────────────────────────────────────────┘"
    echo ""
    
    MSG_NUM=0
    
    docker exec $(docker ps -qf "name=kafka") kafka-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic cdc-dead-letter-queue \
      --from-beginning \
      --max-messages 5 \
      --timeout-ms 3000 2>/dev/null | \
      while read msg; do
        MSG_NUM=$((MSG_NUM + 1))
        
        echo "  📨 DLQ MESSAGE #$MSG_NUM"
        echo "  ┌─────────────────────────────────────────────────────────────────────────────┐"
        echo "  │ Message: ${msg:0:150}..."
        echo "  ├─────────────────────────────────────────────────────────────────────────────┤"
        
        if echo "$msg" | grep -q "Missing action field"; then
            echo "  │ ❌ ERROR TYPE: Missing Required Field"
            echo "  │ 🔍 Missing Field: 'action'"
            echo "  │ 📋 Required Values: 'INSERT', 'UPDATE', 'DELETE'"
            echo "  │ 💡 Root Cause: Producer sent message without action field"
            echo "  │ 🔧 Fix: Ensure producer includes action field in all messages"
            
        elif echo "$msg" | grep -q "Missing emp_id"; then
            echo "  │ ❌ ERROR TYPE: Missing Required Field"
            echo "  │ 🔍 Missing Field: 'emp_id'"
            echo "  │ 📋 Expected Type: Integer"
            echo "  │ 💡 Root Cause: CDC record may have NULL emp_id"
            echo "  │ 🔧 Fix: Verify employees table has valid emp_id values"
            
        elif echo "$msg" | grep -q "malformed\|Malformed"; then
            echo "  │ ❌ ERROR TYPE: Malformed JSON"
            echo "  │ 🔍 Issue: JSON parsing failed"
            echo "  │ 📋 Common Causes: Missing quotes, trailing commas, invalid escape characters"
            echo "  │ 💡 Root Cause: Producer generated invalid JSON"
            echo "  │ 🔧 Fix: Validate JSON before sending to Kafka"
            
        elif echo "$msg" | grep -q "schema\|Schema\|validation\|Validation"; then
            echo "  │ ❌ ERROR TYPE: Schema Validation Failed"
            echo "  │ 🔍 Issue: Message does not match Avro schema"
            echo "  │ 📋 Expected fields: cdc_id, emp_id, first_name, last_name, salary, action"
            echo "  │ 💡 Root Cause: Data in PostgreSQL violates Avro schema constraints"
            echo "  │ 🔧 Fix: Add constraints to PostgreSQL or update Avro schema"
            
        elif echo "$msg" | grep -q "deserialize\|Deserialize"; then
            echo "  │ ❌ ERROR TYPE: Avro Deserialization Failed"
            echo "  │ 🔍 Issue: Cannot parse Avro binary format"
            echo "  │ 💡 Root Cause: Message was not properly serialized with Avro"
            echo "  │ 🔧 Fix: Use Avro serializer before sending to Kafka"
            
        elif echo "$msg" | grep -q "error"; then
            ERROR=$(echo "$msg" | grep -o '"error":"[^"]*"' | cut -d'"' -f4)
            echo "  │ ❌ ERROR TYPE: Generic Error"
            echo "  │ 🔍 Error Message: $ERROR"
            echo "  │ 🔧 Fix: Check producer logs for more details"
            
        else
            echo "  │ ❌ ERROR TYPE: Unknown Format"
            echo "  │ 🔍 Issue: Message structure is invalid"
            echo "  │ 🔧 Fix: Ensure message matches expected Avro schema"
        fi
        
        echo "  └─────────────────────────────────────────────────────────────────────────────┘"
        echo ""
    done
    
    echo "  📋 SUMMARY: These messages were sent to Kafka but FAILED to be processed by consumers"
    echo "              They are stored in DLQ for manual inspection and reprocessing."
fi

echo ""

# ============================================
# 5. PRODUCER OFFSET STATUS
# ============================================
echo "📌 PRODUCER OFFSET STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f producer_offset.txt ]; then
    PRODUCER_OFFSET=$(cat producer_offset.txt)
    echo "  📍 Last processed CDC record: $PRODUCER_OFFSET"
    
    LATEST_CDC=$(docker exec $(docker ps -qf "name=source_db") psql -U postgres -d sourcedb -t -c "SELECT MAX(cdc_id) FROM emp_cdc;" 2>/dev/null | tr -d ' ')
    if [ -n "$LATEST_CDC" ] && [ "$LATEST_CDC" != " " ]; then
        PENDING=$((LATEST_CDC - PRODUCER_OFFSET))
        if [ $PENDING -eq 0 ]; then
            echo "  ✅ No pending records to send"
        else
            echo "  ⏳ $PENDING records pending to send (CDC table has new records)"
        fi
    fi
else
    echo "  ⚠️  Producer offset file not found (producer may not have run yet)"
fi

echo ""

# ============================================
# 6. END-TO-END SYNC VERIFICATION
# ============================================
echo "📌 END-TO-END SYNC VERIFICATION"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SOURCE_COUNT=$(docker exec $(docker ps -qf "name=source_db") psql -U postgres -d sourcedb -t -c "SELECT COUNT(*) FROM employees;" 2>/dev/null | tr -d ' ')
DEST_COUNT=$(docker exec $(docker ps -qf "name=dest_db") psql -U postgres -d destdb -t -c "SELECT COUNT(*) FROM employees;" 2>/dev/null | tr -d ' ')

echo "  Source DB (sourcedb): $SOURCE_COUNT employees"
echo "  Destination DB (destdb): $DEST_COUNT employees"

if [ "$SOURCE_COUNT" = "$DEST_COUNT" ]; then
    echo "  ✅ Source and destination are in sync"
else
    DIFF=$((SOURCE_COUNT - DEST_COUNT))
    echo "  ⚠️  Difference: $DIFF records (records that failed validation or are pending)"
    echo ""
    echo "  Possible reasons for mismatch:"
    echo "    • Records failed Avro schema validation (see SCHEMA REGISTRY ERRORS above)"
    echo "    • Records are in Dead Letter Queue (see DLQ section above)"
    echo "    • Producer hasn't processed pending CDC records"
fi

echo ""

# ============================================
# 7. SUMMARY OF FAILURES
# ============================================
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                           SUMMARY OF FAILURES                                 ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"

FAILURE_COUNT=0

# Count schema errors
SCHEMA_ERROR_COUNT=$(docker exec $(docker ps -qf "name=source_db") psql -U postgres -d sourcedb -t -c "
SELECT COUNT(*) FROM emp_cdc 
WHERE action NOT IN ('INSERT', 'UPDATE', 'DELETE')
   OR first_name IS NULL OR first_name = ''
   OR salary IS NULL OR salary < 0;" 2>/dev/null | tr -d ' ')

if [ -n "$SCHEMA_ERROR_COUNT" ] && [ "$SCHEMA_ERROR_COUNT" != "0" ]; then
    echo "  ❌ Schema Validation Failures: $SCHEMA_ERROR_COUNT records failed"
    echo "     → These records exist in source but never reached destination"
    FAILURE_COUNT=$((FAILURE_COUNT + 1))
else
    echo "  ✅ Schema Validation: No failures"
fi

# DLQ count
if [ -n "$DLQ_COUNT" ] && [ "$DLQ_COUNT" != "0" ]; then
    echo "  ❌ Dead Letter Queue: $DLQ_COUNT messages failed"
    echo "     → These messages reached Kafka but failed consumer processing"
    FAILURE_COUNT=$((FAILURE_COUNT + 1))
else
    echo "  ✅ Dead Letter Queue: Empty"
fi

# Sync status
if [ "$SOURCE_COUNT" != "$DEST_COUNT" ]; then
    DIFF=$((SOURCE_COUNT - DEST_COUNT))
    echo "  ⚠️  Sync Mismatch: $DIFF records difference between source and destination"
    FAILURE_COUNT=$((FAILURE_COUNT + 1))
else
    echo "  ✅ Sync Status: Source and destination match"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ $FAILURE_COUNT -eq 0 ]; then
    echo "🎉 PIPELINE STATUS: HEALTHY - No failures detected"
else
    echo "⚠️  PIPELINE STATUS: DEGRADED - $FAILURE_COUNT issue(s) detected"
    echo "   Review sections above for details and fix recommendations"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""