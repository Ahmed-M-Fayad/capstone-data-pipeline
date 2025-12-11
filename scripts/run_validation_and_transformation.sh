#!/bin/bash
# Run validator and transformer in sequence

DATE=${1:-$(date +%Y-%m-%d)}

echo "================================================"
echo "ETL Pipeline: Validation + Transformation"
echo "Date: $DATE"
echo "================================================"

echo ""
echo "Step 1: Running Validator..."
python3.11 /opt/capstone-pipeline/scripts/validator.py $DATE
VALIDATOR_EXIT=$?

if [ $VALIDATOR_EXIT -ne 0 ]; then
    echo "❌ Validator failed, stopping pipeline"
    exit 1
fi

echo ""
echo "Step 2: Running Transformer..."
python3.11 /opt/capstone-pipeline/scripts/transformer.py $DATE
TRANSFORMER_EXIT=$?

if [ $TRANSFORMER_EXIT -ne 0 ]; then
    echo "❌ Transformer failed"
    exit 1
fi

echo ""
echo "================================================"
echo "✅ Pipeline completed successfully"
echo "================================================"