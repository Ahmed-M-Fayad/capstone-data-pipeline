# AWS Data Pipeline - Capstone Project

## EC2-Based ETL with S3 Data Lake + Dual Storage

**Student:** voclabs/user4605190@f-eng.tanta.edu.eg  
**Account ID:** 590183856719  
**Architecture:** EC2-based ETL processing with multi-zone S3 data lake

## Project Overview

Production-grade data pipeline processing 500K daily transactions with:

- Automated ETL (Extract, Transform, Load)
- Multi-zone data lake (raw → processed → aggregates)
- Dual storage: DynamoDB (fast queries) + RDS PostgreSQL (analytics)
- Cost optimized: ~$57/month

## Quick Stats

- **Processing:** 125K records/batch
- **Storage:** S3 (data lake) + DynamoDB + RDS
- **Compute:** EC2 t3.small with Python
- **Automation:** Cron-based scheduling
- **Monitoring:** CloudWatch

## Architecture

See `docs/architecture/` for diagrams and details.

## Daily Progress

- [Day 1](docs/daily-logs/day-01.md) - Foundation & S3 Setup
- More to come...

## Technologies Used

- **Cloud:** AWS (S3, EC2, DynamoDB, RDS, CloudWatch)
- **Language:** Python 3.11
- **Libraries:** pandas, boto3, psycopg2
- **Automation:** Cron
