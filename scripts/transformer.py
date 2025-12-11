#!/usr/bin/env python3.11
"""
Data Transformer - Stage 2 of ETL Pipeline
Enriches validated sales data with calculated fields and business logic

Features:
- Revenue calculation (quantity × price)
- Date component extraction (year, month, quarter, day_of_week)
- Revenue categorization (Low/Medium/High/Premium)
- Product category mapping
- Customer segment analysis
- Detailed transformation metrics
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict
import pandas as pd
import boto3
from io import StringIO
import numpy as np

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
        logging.FileHandler(f'{config.LOG_DIR}/transformer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3', region_name=config.AWS_REGION)


class DataTransformer:
    """Transforms and enriches validated sales data"""
    
    # Product category mapping
    PRODUCT_CATEGORIES = {
        'Laptop': 'Computing',
        'Desktop': 'Computing',
        'Monitor': 'Peripherals',
        'Keyboard': 'Peripherals',
        'Mouse': 'Peripherals',
        'Headset': 'Audio',
        'Webcam': 'Video',
        'Router': 'Networking',
        'Switch': 'Networking',
        'Cable': 'Accessories'
    }
    
    # Revenue tier thresholds
    REVENUE_TIERS = {
        'Low': (0, 100),
        'Medium': (100, 500),
        'High': (500, 2000),
        'Premium': (2000, float('inf'))
    }
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.metrics = {
            'records_processed': 0,
            'columns_added': 0,
            'avg_revenue': 0,
            'total_revenue': 0,
            'transformation_time': 0
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
            
            logger.info(f"Successfully wrote enriched data to S3")
            
        except Exception as e:
            logger.error(f"Failed to write to S3: {str(e)}")
            raise
    
    def calculate_revenue(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate revenue = quantity × price"""
        logger.info("Calculating revenue...")
        
        df['revenue'] = df['quantity'] * df['price']
        df['revenue'] = df['revenue'].round(2)
        
        self.metrics['total_revenue'] = df['revenue'].sum()
        self.metrics['avg_revenue'] = df['revenue'].mean()
        
        logger.info(f"Total revenue: ${self.metrics['total_revenue']:,.2f}")
        logger.info(f"Average revenue per transaction: ${self.metrics['avg_revenue']:.2f}")
        
        return df
    
    def extract_date_components(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract year, month, quarter, day_of_week from date"""
        logger.info("Extracting date components...")
        
        # Convert to datetime for extraction
        df['date'] = pd.to_datetime(df['date'])
        
        # Extract components
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['month_name'] = df['date'].dt.strftime('%B')
        df['quarter'] = df['date'].dt.quarter
        df['day_of_week'] = df['date'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['day_name'] = df['date'].dt.strftime('%A')
        df['week_of_year'] = df['date'].dt.isocalendar().week
        
        # Add business day flag (True for Mon-Fri)
        df['is_business_day'] = df['day_of_week'] < 5
        
        # Convert date back to string for consistency
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        logger.info("Date components extracted successfully")
        
        return df
    
    def categorize_revenue(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize transactions by revenue tier"""
        logger.info("Categorizing revenue...")
        
        def get_revenue_category(revenue):
            for category, (min_val, max_val) in self.REVENUE_TIERS.items():
                if min_val <= revenue < max_val:
                    return category
            return 'Premium'  # Fallback
        
        df['revenue_category'] = df['revenue'].apply(get_revenue_category)
        
        # Log distribution
        category_dist = df['revenue_category'].value_counts()
        logger.info("Revenue category distribution:")
        for category, count in category_dist.items():
            pct = (count / len(df)) * 100
            logger.info(f"  {category}: {count:,} ({pct:.1f}%)")
        
        return df
    
    def add_product_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map products to categories"""
        logger.info("Adding product categories...")
        
        df['product_category'] = df['product'].map(self.PRODUCT_CATEGORIES)
        
        # Handle unmapped products (if any)
        unmapped = df['product_category'].isna().sum()
        if unmapped > 0:
            logger.warning(f"Found {unmapped} products without category mapping")
            df['product_category'].fillna('Other', inplace=True)
        
        # Log distribution
        category_dist = df['product_category'].value_counts()
        logger.info("Product category distribution:")
        for category, count in category_dist.items():
            pct = (count / len(df)) * 100
            logger.info(f"  {category}: {count:,} ({pct:.1f}%)")
        
        return df
    
    def add_customer_segments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Segment customers based on purchase behavior"""
        logger.info("Adding customer segments...")
        
        # Calculate per-customer metrics
        customer_stats = df.groupby('customer_id').agg({
            'revenue': 'sum',
            'transaction_id': 'count'
        }).rename(columns={'transaction_id': 'purchase_count'})
        
        # Define segments based on total revenue
        def get_customer_segment(total_revenue):
            if total_revenue < 500:
                return 'Bronze'
            elif total_revenue < 2000:
                return 'Silver'
            elif total_revenue < 5000:
                return 'Gold'
            else:
                return 'Platinum'
        
        customer_stats['customer_segment'] = customer_stats['revenue'].apply(
            get_customer_segment
        )
        
        # Merge back to main dataframe
        df = df.merge(
            customer_stats[['customer_segment']], 
            left_on='customer_id', 
            right_index=True, 
            how='left'
        )
        
        # Log distribution
        segment_dist = df['customer_segment'].value_counts()
        logger.info("Customer segment distribution:")
        for segment, count in segment_dist.items():
            pct = (count / len(df)) * 100
            logger.info(f"  {segment}: {count:,} ({pct:.1f}%)")
        
        return df
    
    def add_price_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add price analysis indicators"""
        logger.info("Adding price indicators...")
        
        # Calculate price percentile for each product
        df['price_percentile'] = df.groupby('product')['price'].rank(pct=True)
        
        # Flag high-value transactions (top 10% by revenue)
        revenue_90th = df['revenue'].quantile(0.9)
        df['is_high_value'] = df['revenue'] >= revenue_90th
        
        high_value_count = df['is_high_value'].sum()
        logger.info(f"High-value transactions: {high_value_count:,} "
                   f"({(high_value_count/len(df)*100):.1f}%)")
        
        return df
    
    def add_quantity_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add quantity-based indicators"""
        logger.info("Adding quantity indicators...")
        
        # Flag bulk purchases (quantity > median for that product)
        product_medians = df.groupby('product')['quantity'].transform('median')
        df['is_bulk_purchase'] = df['quantity'] > product_medians
        
        # Calculate quantity percentile
        df['quantity_percentile'] = df.groupby('product')['quantity'].rank(pct=True)
        
        bulk_count = df['is_bulk_purchase'].sum()
        logger.info(f"Bulk purchases: {bulk_count:,} "
                   f"({(bulk_count/len(df)*100):.1f}%)")
        
        return df
    
    def add_regional_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add region-based analysis"""
        logger.info("Adding regional indicators...")
        
        # Calculate regional averages
        regional_avg = df.groupby('region')['revenue'].transform('mean')
        df['regional_avg_revenue'] = regional_avg.round(2)
        
        # Flag if transaction is above regional average
        df['above_regional_avg'] = df['revenue'] > df['regional_avg_revenue']
        
        # Log regional performance
        regional_stats = df.groupby('region')['revenue'].agg(['sum', 'mean', 'count'])
        logger.info("Regional performance:")
        for region in regional_stats.index:
            total = regional_stats.loc[region, 'sum']
            avg = regional_stats.loc[region, 'mean']
            count = regional_stats.loc[region, 'count']
            logger.info(f"  {region}: {count:,} txns, "
                       f"${total:,.2f} total, ${avg:.2f} avg")
        
        return df
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations"""
        logger.info("Starting transformation pipeline...")
        
        initial_columns = len(df.columns)
        
        # Apply transformations
        df = self.calculate_revenue(df)
        df = self.extract_date_components(df)
        df = self.categorize_revenue(df)
        df = self.add_product_categories(df)
        df = self.add_customer_segments(df)
        df = self.add_price_indicators(df)
        df = self.add_quantity_indicators(df)
        df = self.add_regional_indicators(df)
        
        final_columns = len(df.columns)
        self.metrics['columns_added'] = final_columns - initial_columns
        self.metrics['records_processed'] = len(df)
        
        logger.info(f"Transformation complete: {initial_columns} → {final_columns} columns "
                   f"({self.metrics['columns_added']} added)")
        
        return df
    
    def process_file(self, input_key: str, output_key: str) -> Dict:
        """Main transformation pipeline"""
        try:
            logger.info(f"Starting transformation for {input_key}")
            start_time = datetime.now()
            
            # Read validated data
            df = self.read_from_s3(input_key)
            
            # Transform data
            df = self.transform_data(df)
            
            # Write enriched data
            self.write_to_s3(df, output_key)
            
            # Calculate metrics
            processing_time = (datetime.now() - start_time).total_seconds()
            self.metrics['transformation_time'] = processing_time
            
            logger.info(f"Transformation completed in {processing_time:.2f} seconds")
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Transformation pipeline failed: {str(e)}")
            raise
    
    def print_metrics_summary(self):
        """Print transformation metrics"""
        logger.info("\n" + "="*60)
        logger.info("TRANSFORMATION METRICS SUMMARY")
        logger.info("="*60)
        logger.info(f"Records Processed:      {self.metrics['records_processed']:,}")
        logger.info(f"Columns Added:          {self.metrics['columns_added']}")
        logger.info(f"Total Revenue:          ${self.metrics['total_revenue']:,.2f}")
        logger.info(f"Average Revenue:        ${self.metrics['avg_revenue']:.2f}")
        logger.info(f"Processing Time:        {self.metrics['transformation_time']:.2f}s")
        logger.info("="*60 + "\n")


def main():
    """Main execution function"""
    
    # Default to today's date if no argument provided
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Processing date: {date_str}")
    
    # Define S3 keys (read and write to processed-zone)
    input_key = f"{config.PROCESSED_ZONE}/{date_str}.csv"
    output_key = f"{config.PROCESSED_ZONE}/{date_str}.csv"  # Overwrite
    
    try:
        # Initialize transformer
        transformer = DataTransformer(config.S3_BUCKET)
        
        # Process file
        metrics = transformer.process_file(input_key, output_key)
        
        # Print summary
        transformer.print_metrics_summary()
        
        logger.info("✅ Transformer completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Transformer failed: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())