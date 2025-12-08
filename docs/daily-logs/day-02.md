# Day 2: EC2 Instance Setup (Console Method)

## Completed Tasks

✅ Created security group `capstone-etl-sg` with SSH access
✅ Created key pair `capstone-etl-key.pem`
✅ Launched EC2 instance: t3.small with Amazon Linux 2023
✅ Attached IAM role `myS3Role` for S3 access
✅ Bootstrap script installed:

- Python 3.11
- pandas, boto3, psycopg2-binary, numpy, requests
- CloudWatch agent
  ✅ Verified S3 access from EC2
  ✅ Created test script confirming end-to-end connectivity

## Resources Created

- **EC2 Instance:** i-xxxxxxxxxxxxx (t3.small)
- **Public IP:** X.X.X.X
- **Security Group:** sg-xxxxxxxxxxxxx (capstone-etl-sg)
- **Key Pair:** capstone-etl-key
- **IAM Profile:** myS3Role (pre-existing)

## Technical Verification

```bash
# Python version
Python 3.11.6

# S3 access confirmed
aws s3 ls s3://capstone-datalake-590183856719/
✅ Can read raw-zone/2025-12-05.csv

# IAM role confirmed
arn:aws:sts::590183856719:assumed-role/myS3Role/i-xxxxxxxxxxxxx
```

## Cost Tracking

| Service         | Configuration | Daily Cost |
| --------------- | ------------- | ---------- |
| EC2 t3.small    | 24 hours      | $0.51      |
| S3 Storage      | 8 MB          | $0.00      |
| **Total Day 2** |               | **$0.51**  |

## Issues Encountered

[None / or describe any issues and how you solved them]

## Tomorrow's Plan (Day 3)

- Write `validator.py` script
  - Read CSV from raw-zone
  - Remove NULL values
  - Remove duplicates by transaction_id
  - Validate data types
  - Write cleaned data to processed-zone
- Test with 2025-12-05.csv (125K records)
- Measure processing time and rejection rate

## Screenshots

- [Screenshot 1: EC2 instance running]
- [Screenshot 2: Security group configuration]
- [Screenshot 3: SSH connection successful]
- [Screenshot 4: S3 test script output]
