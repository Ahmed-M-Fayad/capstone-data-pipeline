# AWS Data Pipeline - Capstone Project
## EC2-Based ETL with S3 Data Lake + Dual Storage

**Student:** voclabs/user4605190@f-eng.tanta.edu.eg  
**Account ID:** 590183856719  
**Architecture:** EC2-based ETL processing with multi-zone S3 data lake

---

## Project Overview

Production-grade data pipeline processing 500K daily transactions with:
- Automated ETL (Extract, Transform, Load)
- Multi-zone data lake (raw → processed → aggregates)
- Dual storage: DynamoDB (fast queries) + RDS PostgreSQL (analytics)
- Cost optimized: ~$57/month

---

## Quick Stats

- **Processing:** 125K records/batch
- **Storage:** S3 (data lake) + DynamoDB + RDS
- **Compute:** EC2 t3.small with Python
- **Automation:** Cron-based scheduling
- **Monitoring:** CloudWatch

---

## Resources Created

### ✅ Day 1: S3 Data Lake
```
Bucket: capstone-datalake-590183856719
Region: us-east-1
Structure:
  ├── raw-zone/           # Landing zone for incoming files
  ├── processed-zone/     # Validated and cleaned data
  └── aggregates-zone/    # Analytics-ready rollups

Features:
  • AES-256 encryption
  • Versioning enabled
  • Lifecycle policy: Glacier after 90 days
  • Test data: 125K records (2025-12-05.csv)
```

### ✅ Day 2: EC2 ETL Worker
```
Instance: capstone-etl-worker (i-0ae8e466c0135c946)
Type: t3.small (2 vCPU, 2 GB RAM)
AMI: Amazon Linux 2023
IAM Role: myS3Role
Security: capstone-etl-sg (SSH access)

Environment:
  • Python 3.11
  • pandas, boto3, psycopg2-binary, numpy, requests
  • CloudWatch agent
  • S3 access verified ✅

Directory: /opt/capstone-pipeline/
  ├── scripts/    # ETL scripts
  ├── logs/       # Application logs
  ├── config/     # Configuration files
  └── data/       # Temporary processing
```

---

## Daily Progress

- **[Day 1](docs/daily-logs/day-01.md)** - S3 Data Lake Setup
- **[Day 2](docs/daily-logs/day-02.md)** - EC2 Instance Setup

---

## Technologies Used

- **Cloud:** AWS (S3, EC2, IAM)
- **Language:** Python 3.11
- **Libraries:** pandas, boto3, psycopg2, numpy

---

## Current Status

✅ **Phase 0: Foundation Complete**
- S3 bucket with 3-zone structure
- Test data (125K records)
- EC2 instance configured
- S3 access verified

⏳ **Next: Phase 1 - ETL Scripts**

---

## Contact

For questions or feedback: ahmedfayad390@gmail.com
