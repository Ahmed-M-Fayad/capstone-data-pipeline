# Day 1: Foundation & S3 Data Lake Setup

**Date:** December 6, 2025  
**Time Spent:** 4 hours  
**Status:** ✅ Complete

---

## 🎯 Objectives

- Set up project repository and structure
- Create S3 data lake with multi-zone architecture
- Generate and upload test data
- Configure security and lifecycle policies

---

## ✅ Completed Tasks

### 1. GitHub Repository

- Created project structure with organized folders
- Initialized Git repository
- Pushed to GitHub: https://github.com/Ahmed-M-Fayad/capstone-data-pipeline
- Structure includes: docs/, scripts/, config/, test-data/

### 2. S3 Data Lake

- **Bucket Created:** `capstone-datalake-590183856719`
- **Region:** us-east-1
- **Architecture:** 3-zone data lake
  - `raw-zone/` - Landing zone for incoming data
  - `processed-zone/` - Validated and cleaned data
  - `aggregates-zone/` - Analytics-ready rollups

### 3. Security Configuration

- ✅ Versioning enabled (data recovery)
- ✅ AES-256 encryption enabled
- ✅ Public access blocked (all 4 settings)
- ✅ Lifecycle policy: Glacier transition after 90 days

### 4. Test Data

- Generated 125,000 sales transaction records
- File size: ~8.5 MB
- Includes intentional data quality issues:
  - ~2% duplicates (for validator testing)
  - ~1% null values (for validator testing)
- Uploaded to: `s3://capstone-datalake-590183856719/raw-zone/2025-12-06.csv`

---

## 📊 Resources Created

| Resource    | ARN/ID                         | Purpose           |
| ----------- | ------------------------------ | ----------------- |
| S3 Bucket   | capstone-datalake-590183856719 | Data lake storage |
| GitHub Repo | [URL]                          | Version control   |
| Test Data   | 2025-12-06.csv                 | Pipeline testing  |

---

## 💰 Cost Tracking

### Day 1 Costs

- S3 Storage: <$0.01 (8.5 MB)
- S3 Requests: <$0.01 (1 PUT request)
- **Total Day 1:** ~$0.01

### Projected Monthly (S3 only so far)

- Storage (50GB avg): $1.15
- Requests (1M GET, 100K PUT): $0.50
- **S3 Subtotal:** ~$1.65/month

---

## 🐛 Issues Encountered

None - smooth setup! 🎉

---

## 📸 Screenshots

See `docs/screenshots/`:

- [ ] S3 bucket overview with 3 folders
- [ ] Bucket properties (versioning, encryption)
- [ ] Lifecycle policy configuration
- [ ] Generation of test data
- [ ] Test data file in raw-zone

---

## 📚 What I Learned

1. **Data Lake Architecture:** Multi-zone pattern separates concerns
2. **S3 Security:** Multiple layers (encryption, versioning, access control)
3. **Cost Optimization:** Lifecycle policies reduce long-term storage costs
4. **Test Data Generation:** Importance of realistic test scenarios

---

## 🎯 Tomorrow's Plan (Day 2)

### Objectives

- Launch EC2 instance (t3.small)
- Configure instance profile with myS3Role
- Install Python 3.11 and dependencies
- Verify S3 access from EC2
- Clone GitHub repo to instance

### Estimated Time

4-5 hours

### Prerequisites

- ✅ S3 bucket operational
- ✅ Test data available
- ✅ GitHub repo accessible

---

## 📝 Notes

- Using AWS Console instead of CLI for better learning
- Multi-zone architecture follows data lake best practices
- Intentional data quality issues will test our validator script
- Ready to move to compute layer (EC2) tomorrow!
