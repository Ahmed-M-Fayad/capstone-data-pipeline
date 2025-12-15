# Day 6: Pipeline Orchestrator

## Date: 2025-12-15

## Objectives

✅ Create orchestrator.py to run all ETL stages
✅ Implement error handling and stage tracking
✅ Add execution reporting
✅ Test end-to-end pipeline

## Tasks Completed

### 1. Orchestrator Development

Created orchestrator.py with features:

- Sequential stage execution (validator → transformer → aggregator)
- Error handling with graceful failures
- Stage duration tracking
- Comprehensive logging
- Execution reports saved to S3
- Timeout protection (5 min per stage)

### 2. Pipeline Testing

```bash
python3.11 orchestrator.py 2025-12-05
```

**Results:**

- ✓ All 3 stages completed successfully
- Total duration: ~9.00 seconds
- Pipeline status: SUCCESS
- Execution report: pipeline-reports/2025-12-05.json

### 3. Error Handling Test

```bash
python3.11 orchestrator.py 2025-01-01  # Non-existent file
```

**Results:**

- ✓ Pipeline detected missing input file
- ✓ Failed gracefully without running stages
- ✓ Error logged and reported correctly

### 4. S3 Structure Verification

Current S3 bucket structure:

```
capstone-datalake-590183856719/
├── raw-zone/
│   └── 2025-12-05.csv
├── processed-zone/
│   └── 2025-12-05.csv (validated + transformed)
├── aggregates-zone/
│   ├── daily/2025-12-05.json
│   ├── regional/2025-12-05.json
│   └── products/2025-12-05.json
└── pipeline-reports/
    └── 2025-12-05.json
```

## Pipeline Metrics (2025-12-05)

- **Execution Time:** 8.45 seconds
- **Validator:** 1.93s
- **Transformer:** 4.34s
- **Aggregator:** 2.00s
- **Total Transactions:** 121,278
- **Total Revenue:** $1,560,293,855.19
- **Pass Rate:** 99.8%

## Key Features Implemented

1. **Sequential Execution:** Each stage runs only if previous succeeds
2. **Timeout Protection:** 5-minute limit per stage
3. **Error Propagation:** Failures stop pipeline and skip remaining stages
4. **Metrics Collection:** Automatically collects and reports key metrics
5. **Execution Reports:** JSON reports saved to S3 for analysis
6. **Comprehensive Logging:** All actions logged with timestamps

## Lessons Learned

- subprocess.run() is better than os.system() for capturing output
- Timeout protection prevents hanging scripts
- JSON execution reports enable pipeline monitoring/alerting
- Sequential execution with early failure prevents data corruption

## Issues Encountered

- None

## Tomorrow's Plan (Day 7)

- Set up DynamoDB table for fast metrics queries
- Create table schema with date-region composite key
- Write data loader to populate DynamoDB from aggregates
- Test query performance

## Files Modified

- scripts/orchestrator.py (new)
- logs/orchestrator.log (new)
- S3: pipeline-reports/ (new folder)

## Command Reference

```bash
# Run full pipeline for specific date
python3.11 orchestrator.py 2025-12-05

# Run for today's date
python3.11 orchestrator.py

# View execution report
aws s3 cp s3://capstone-datalake-590183856719/pipeline-reports/2025-12-05.json -

# View all logs
tail -f /opt/capstone-pipeline/logs/orchestrator.log
```

## Next Steps

- Automate with cron (Day 10)
- Add email notifications (optional)
- Create monitoring dashboard (Day 12)
