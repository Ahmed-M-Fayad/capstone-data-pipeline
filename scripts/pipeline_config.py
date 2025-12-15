"""
Pipeline Configuration
Centralized settings for all ETL scripts
"""

# AWS Configuration
AWS_REGION = 'us-east-1'
S3_BUCKET = 'capstone-datalake-590183856719'

# S3 Zones
RAW_ZONE = 'raw-zone'
PROCESSED_ZONE = 'processed-zone'
AGGREGATES_ZONE = 'aggregates-zone'

# Data Schema
REQUIRED_COLUMNS = [
    'transaction_id',
    'date',
    'region',
    'product',
    'quantity',
    'price',
    'customer_id'
]

EXPECTED_DTYPES = {
    'transaction_id': 'object',
    'date': 'object',
    'region': 'object',
    'product': 'object',
    'quantity': 'int64',
    'price': 'float64',
    'customer_id': 'object'
}

# Validation Rules
MIN_QUANTITY = 1
MAX_QUANTITY = 1000
MIN_PRICE = 0.01
MAX_PRICE = 100000.00

VALID_REGIONS = ['North', 'South', 'East', 'West', 'Central']

# Logging Configuration
LOG_DIR = '/opt/capstone-pipeline/logs'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Processing Settings
BATCH_SIZE = 10000  # Process in chunks for memory efficiency
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds