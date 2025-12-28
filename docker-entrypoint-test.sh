#!/bin/bash
set -e

echo "=================================================="
echo "  Nexus Test Suite - Data Quality Validation"
echo "=================================================="

# Check if database exists
if [ ! -f "/app/data/processed/forest.db" ]; then
    echo "❌ ERROR: Database not found at /app/data/processed/forest.db"
    echo ""
    echo "Please ensure:"
    echo "  1. Pipeline has run successfully"
    echo "  2. Database volume is mounted correctly"
    exit 1
fi

echo "✅ Database found: /app/data/processed/forest.db"
echo "   Size: $(du -h /app/data/processed/forest.db | cut -f1)"
echo ""

# Parse command line arguments
TEST_SUITE="${1:-all}"

echo "🧪 Running test suite: $TEST_SUITE"
echo ""

case $TEST_SUITE in
    "unit")
        echo "Running unit tests..."
        uv run pytest tests/unit/ -v --no-cov
        ;;
    "integration")
        echo "Running integration tests..."
        uv run pytest tests/integration/ -v --no-cov
        ;;
    "database")
        echo "Running database validation tests..."
        uv run pytest tests/integration/test_database.py -v --no-cov
        ;;
    "all")
        echo "Running all tests..."
        uv run pytest tests/ -v --no-cov
        ;;
    *)
        echo "Unknown test suite: $TEST_SUITE"
        echo "Valid options: unit, integration, database, all"
        exit 1
        ;;
esac

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=================================================="
    echo "✅ All tests passed!"
    echo "=================================================="
else
    echo ""
    echo "=================================================="
    echo "❌ Tests failed with exit code $EXIT_CODE"
    echo "=================================================="
    exit $EXIT_CODE
fi