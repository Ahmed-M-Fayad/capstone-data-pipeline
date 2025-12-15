# Day 7: Analytics Dashboard

## Date: 2025-12-15

## Architecture Adjustment Log

### DynamoDB Integration Limitation

- The initial plan to integrate **Amazon DynamoDB** for low-latency metrics querying could not be completed.
- The failure was caused by **sandbox environment restrictions**, which prevented successful table creation and access.
- This was identified as an environmental limitation rather than an issue with schema design, IAM configuration, or application logic.

### Design Pivot: Static Analytics Dashboard

- To maintain project momentum and meet observability requirements, the system architecture was adjusted.
- The DynamoDB-based metrics layer was replaced with a **static analytics dashboard hosted on Amazon S3**.
- The dashboard retrieves **pre-aggregated and cleaned datasets** directly from the data lake (`aggregates-zone`).
- This approach ensures:
  - Read-only, controlled access to analytics data
  - Minimal operational overhead
  - Full compatibility with static website hosting constraints

### Outcome

- The dashboard successfully serves analytical metrics without reliance on managed database services.
- The overall architecture remains **fully serverless, secure, and cost-efficient**.
- Aggregated datasets continue to function as the single source of truth for reporting and visualization.
