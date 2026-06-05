#!/bin/bash

echo "════════════════════════════════════════════════════════════"
echo "🧹 CLEANING UP CDC PIPELINE - COMPLETE RESET"
echo "════════════════════════════════════════════════════════════"
echo ""

# 1. Stop all Docker containers
echo "1️⃣ Stopping Docker containers..."
docker-compose down -v 2>/dev/null
docker-compose -f docker-compose-airflow.yml down -v 2>/dev/null

# 2. Remove all project containers
echo "2️⃣ Removing project containers..."
docker rm -f $(docker ps -a -q --filter "name=project2-cdc") 2>/dev/null
docker rm -f $(docker ps -a -q --filter "name=airflow") 2>/dev/null

# 3. Remove volumes
echo "3️⃣ Removing volumes..."
docker volume rm $(docker volume ls -q --filter "name=project2-cdc") 2>/dev/null
docker volume prune -f 2>/dev/null

# 4. Remove network
echo "4️⃣ Removing networks..."
docker network rm project2-cdc_default 2>/dev/null
docker network prune -f 2>/dev/null

# 5. Remove local files
echo "5️⃣ Removing local tracking files..."
rm -f producer_offset.txt
rm -f processed_ids.txt
rm -f consumed_ids.txt
rm -f standalone_admin_password.txt

# 6. Clean Python cache
echo "6️⃣ Cleaning Python cache..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -type f -name "*.pyc" -delete 2>/dev/null

# 7. Remove Kafka topics (handled by Docker down -v)

echo ""
echo "════════════════════════════════════════════════════════════"
echo "✅ CLEANUP COMPLETE!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📋 What was cleaned:"
echo "   ✓ All Docker containers removed"
echo "   ✓ All volumes deleted"
echo "   ✓ Networks removed"
echo "   ✓ Offset files deleted"
echo "   ✓ Processed IDs deleted"
echo "   ✓ Python cache cleaned"
echo ""
echo "🚀 Ready for fresh start!"