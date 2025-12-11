#!/usr/bin/env python3.11
"""
Data Validator - Stage 1 of ETL Pipeline
Cleans and validates raw sales data from S3

Features:
- Schema validation
- Duplicate removal
- Null value handling
- Business rule validation
- Detailed metrics logging
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, Tuple
import pandas as pd
import boto3
from io import StringIO

# Add config to path
sys.path.append('/opt/capstone-pipeline/config')
import pipeline_config as config

# Setup logging
os.makedirs(config.LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format=config.LOG_FORMAT,
    datefmt=config.LOG_DATE_FORMAT,
    handlers=[
        logging.FileHandler(f'{config.LOG_DIR}/validator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3', region_name=config.AWS_REGION)


class DataValidator:
    """Validates and cleans raw sales data"""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.metrics = {
            'total_records': 0,
            'valid_records': 0,
            'duplicates_removed': 0,
            'null_values_removed': 0,
            'invalid_quantity': 0,
            'invalid_price': 0,
            'invalid_region': 0,
            'schema_errors': 0
        }
    
    def read_from_s3(self, key: str) -> pd.DataFrame:
        """Read CSV file from S3"""
        try:
            logger.info(f"Reading file: s3://{self.bucket_name}/{key}")
            
            response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            df = pd.read_csv(StringIO(content))
            logger.info(f"Successfully read {len(df)} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from S3: {str(e)}")
            raise
    
    def write_to_s3(self, df: pd.DataFrame, key: str) -> None:
        """Write DataFrame to S3 as CSV"""
        try:
            logger.info(f"Writing {len(df)} records to s3://{self.bucket_name}/{key}")
            
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=csv_buffer.getvalue()
            )
            
            logger.info(f"Successfully wrote to S3")
            
        except Exception as e:
            logger.error(f"Failed to write to S3: {str(e)}")
            raise
    
    def validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate DataFrame schema"""
        logger.info("Validating schema...")
        
        # Check for required columns
        missing_cols = set(config.REQUIRED_COLUMNS) - set(df.columns)
        if missing_cols:
            self.metrics['schema_errors'] += 1
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Select only required columns (ignore extras)
        df = df[config.REQUIRED_COLUMNS].copy()
        
        logger.info("Schema validation passed")
        return df
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate transaction IDs"""
        logger.info("Checking for duplicates...")
        
        initial_count = len(df)
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates = initial_count - len(df)
        
        self.metrics['duplicates_removed'] = duplicates
        logger.info(f"Removed {duplicates} duplicate records")
        
        return df
    
    def remove_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove records with null values"""
        logger.info("Checking for null values...")
        
        initial_count = len(df)
        df = df.dropna()
        nulls = initial_count - len(df)
        
        self.metrics['null_values_removed'] = nulls
        logger.info(f"Removed {nulls} records with null values")
        
        return df
    
    def validate_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert and validate data types"""
        logger.info("Validating data types...")
        
        try:
            # Convert quantity to integer
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
            
            # Convert price to float
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            
            # Remove records with conversion errors
            initial_count = len(df)
            df = df.dropna(subset=['quantity', 'price'])
            errors = initial_count - len(df)
            
            if errors > 0:
                logger.warning(f"Removed {errors} records due to data type conversion errors")
            
            return df
            
        except Exception as e:
            logger.error(f"Data type validation failed: {str(e)}")
            raise
    
    def validate_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business validation rules"""
        logger.info("Applying business rules...")
        
        initial_count = len(df)
        
        # Validate quantity
        invalid_qty = df[
            (df['quantity'] < config.MIN_QUANTITY) |
            (df['quantity'] > config.MAX_QUANTITY)
        ]
        self.metrics['invalid_quantity'] = len(invalid_qty)
        
        # Validate price
        invalid_price = df[
            (df['price'] < config.MIN_PRICE) |
            (df['price'] > config.MAX_PRICE)
        ]
        self.metrics['invalid_price'] = len(invalid_price)
        
        # Validate region
        invalid_region = df[~df['region'].isin(config.VALID_REGIONS)]
        self.metrics['invalid_region'] = len(invalid_region)
        
        # Keep only valid records
        df = df[
            (df['quantity'] >= config.MIN_QUANTITY) &
            (df['quantity'] <= config.MAX_QUANTITY) &
            (df['price'] >= config.MIN_PRICE) &
            (df['price'] <= config.MAX_PRICE) &
            (df['region'].isin(config.VALID_REGIONS))
        ]
        
        rejected = initial_count - len(df)
        logger.info(f"Business rules rejected {rejected} records")
        
        return df
    
    def validate_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and standardize date format"""
        logger.info("Validating dates...")
        
        try:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Remove records with invalid dates
            initial_count = len(df)
            df = df.dropna(subset=['date'])
            invalid_dates = initial_count - len(df)
            
            if invalid_dates > 0:
                logger.warning(f"Removed {invalid_dates} records with invalid dates")
            
            # Convert back to string format YYYY-MM-DD
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            return df
            
        except Exception as e:
            logger.error(f"Date validation failed: {str(e)}")
            raise
    
    def process_file(self, input_key: str, output_key: str) -> Dict:
        """Main validation pipeline"""
        try:
            logger.info(f"Starting validation pipeline for {input_key}")
            start_time = datetime.now()
            
            # Read data
            df = self.read_from_s3(input_key)
            self.metrics['total_records'] = len(df)
            
            # Validation pipeline
            df = self.validate_schema(df)
            df = self.remove_duplicates(df)
            df = self.remove_nulls(df)
            df = self.validate_data_types(df)
            df = self.validate_dates(df)
            df = self.validate_business_rules(df)
            
            self.metrics['valid_records'] = len(df)
            
            # Write validated data
            self.write_to_s3(df, output_key)
            
            # Calculate metrics
            processing_time = (datetime.now() - start_time).total_seconds()
            rejection_rate = (
                (self.metrics['total_records'] - self.metrics['valid_records']) /
                self.metrics['total_records'] * 100
            )
            
            self.metrics['processing_time_seconds'] = processing_time
            self.metrics['rejection_rate_percent'] = round(rejection_rate, 2)
            
            logger.info(f"Validation completed successfully in {processing_time:.2f} seconds")
            logger.info(f"Valid records: {self.metrics['valid_records']:,} / "
                       f"{self.metrics['total_records']:,} "
                       f"({100-rejection_rate:.2f}% pass rate)")
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Validation pipeline failed: {str(e)}")
            raise
    
    def print_metrics_summary(self):
        """Print validation metrics"""
        logger.info("\n" + "="*60)
        logger.info("VALIDATION METRICS SUMMARY")
        logger.info("="*60)
        logger.info(f"Total Records:          {self.metrics['total_records']:,}")
        logger.info(f"Valid Records:          {self.metrics['valid_records']:,}")
        logger.info(f"Duplicates Removed:     {self.metrics['duplicates_removed']:,}")
        logger.info(f"Null Values Removed:    {self.metrics['null_values_removed']:,}")
        logger.info(f"Invalid Quantity:       {self.metrics['invalid_quantity']:,}")
        logger.info(f"Invalid Price:          {self.metrics['invalid_price']:,}")
        logger.info(f"Invalid Region:         {self.metrics['invalid_region']:,}")
        logger.info(f"Rejection Rate:         {self.metrics['rejection_rate_percent']}%")
        logger.info(f"Processing Time:        {self.metrics['processing_time_seconds']:.2f}s")
        logger.info("="*60 + "\n")


def main():
    """Main execution function"""
    
    # Default to today's date if no argument provided
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Processing date: {date_str}")
    
    # Define S3 keys
    input_key = f"{config.RAW_ZONE}/{date_str}.csv"
    output_key = f"{config.PROCESSED_ZONE}/{date_str}.csv"
    
    try:
        # Initialize validator
        validator = DataValidator(config.S3_BUCKET)
        
        # Process file
        metrics = validator.process_file(input_key, output_key)
        
        # Print summary
        validator.print_metrics_summary()
        
        logger.info("✅ Validator completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Validator failed: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())