#!/usr/bin/env python3.11
"""
Data Aggregator - Stage 3 of ETL Pipeline
Creates summary statistics and rollups for analytics

Features:
- Daily aggregate metrics
- Regional performance summaries
- Product performance analysis
- Time-based trends
- Customer segment summaries
- Outputs to aggregates-zone in JSON format
"""

import sys
import os
import logging
import json
from datetime import datetime
from typing import Dict, List
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
        logging.FileHandler(f'{config.LOG_DIR}/aggregator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3', region_name=config.AWS_REGION)


class DataAggregator:
    """Creates aggregated views of sales data"""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.metrics = {
            'records_aggregated': 0,
            'daily_summary_created': False,
            'regional_summaries_created': 0,
            'product_summaries_created': 0,
            'aggregation_time': 0
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
    
    def write_json_to_s3(self, data: Dict, key: str) -> None:
        """Write JSON data to S3"""
        try:
            logger.info(f"Writing aggregates to s3://{self.bucket_name}/{key}")
            
            json_content = json.dumps(data, indent=2, default=str)
            
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_content,
                ContentType='application/json'
            )
            
            logger.info(f"Successfully wrote aggregates to S3")
            
        except Exception as e:
            logger.error(f"Failed to write to S3: {str(e)}")
            raise
    
    def create_daily_summary(self, df: pd.DataFrame, date_str: str) -> Dict:
        """Create overall daily summary metrics"""
        logger.info("Creating daily summary...")
        
        summary = {
            'date': date_str,
            'generated_at': datetime.now().isoformat(),
            'total_transactions': int(len(df)),
            'total_revenue': float(df['revenue'].sum()),
            'average_revenue': float(df['revenue'].mean()),
            'median_revenue': float(df['revenue'].median()),
            'min_revenue': float(df['revenue'].min()),
            'max_revenue': float(df['revenue'].max()),
            'total_quantity_sold': int(df['quantity'].sum()),
            'average_quantity': float(df['quantity'].mean()),
            'unique_customers': int(df['customer_id'].nunique()),
            'unique_products': int(df['product'].nunique()),
        }
        
        logger.info(f"Daily summary: {summary['total_transactions']:,} transactions, "
                   f"${summary['total_revenue']:,.2f} revenue")
        
        self.metrics['daily_summary_created'] = True
        return summary
    
    def create_regional_summaries(self, df: pd.DataFrame) -> List[Dict]:
        """Create summaries by region"""
        logger.info("Creating regional summaries...")
        
        regional_agg = df.groupby('region').agg({
            'transaction_id': 'count',
            'revenue': ['sum', 'mean', 'median'],
            'quantity': 'sum',
            'customer_id': 'nunique',
            'product': 'nunique'
        }).reset_index()
        
        # Flatten column names
        regional_agg.columns = [
            'region',
            'transaction_count',
            'total_revenue',
            'avg_revenue',
            'median_revenue',
            'total_quantity',
            'unique_customers',
            'unique_products'
        ]
        
        summaries = []
        for _, row in regional_agg.iterrows():
            summary = {
                'region': row['region'],
                'transaction_count': int(row['transaction_count']),
                'total_revenue': float(row['total_revenue']),
                'avg_revenue': float(row['avg_revenue']),
                'median_revenue': float(row['median_revenue']),
                'total_quantity': int(row['total_quantity']),
                'unique_customers': int(row['unique_customers']),
                'unique_products': int(row['unique_products']),
                'revenue_per_customer': float(row['total_revenue'] / row['unique_customers'])
            }
            summaries.append(summary)
            
            logger.info(f"  {summary['region']}: {summary['transaction_count']:,} txns, "
                       f"${summary['total_revenue']:,.2f} revenue")
        
        self.metrics['regional_summaries_created'] = len(summaries)
        return summaries
    
    def create_product_summaries(self, df: pd.DataFrame) -> List[Dict]:
        """Create summaries by product"""
        logger.info("Creating product summaries...")
        
        product_agg = df.groupby('product').agg({
            'transaction_id': 'count',
            'revenue': ['sum', 'mean'],
            'quantity': 'sum',
            'price': 'mean',
            'customer_id': 'nunique'
        }).reset_index()
        
        # Flatten column names
        product_agg.columns = [
            'product',
            'transaction_count',
            'total_revenue',
            'avg_revenue',
            'total_quantity',
            'avg_price',
            'unique_customers'
        ]
        
        # Sort by total revenue descending
        product_agg = product_agg.sort_values('total_revenue', ascending=False)
        
        summaries = []
        for _, row in product_agg.iterrows():
            summary = {
                'product': row['product'],
                'transaction_count': int(row['transaction_count']),
                'total_revenue': float(row['total_revenue']),
                'avg_revenue': float(row['avg_revenue']),
                'total_quantity': int(row['total_quantity']),
                'avg_price': float(row['avg_price']),
                'unique_customers': int(row['unique_customers'])
            }
            summaries.append(summary)
        
        # Log top 5 products
        logger.info("Top 5 products by revenue:")
        for i, summary in enumerate(summaries[:5], 1):
            logger.info(f"  {i}. {summary['product']}: ${summary['total_revenue']:,.2f}")
        
        self.metrics['product_summaries_created'] = len(summaries)
        return summaries
    
    def create_product_category_summaries(self, df: pd.DataFrame) -> List[Dict]:
        """Create summaries by product category"""
        logger.info("Creating product category summaries...")
        
        # Check if product_category column exists (added by transformer)
        if 'product_category' not in df.columns:
            logger.warning("product_category column not found, skipping category summaries")
            return []
        
        category_agg = df.groupby('product_category').agg({
            'transaction_id': 'count',
            'revenue': ['sum', 'mean'],
            'quantity': 'sum',
            'customer_id': 'nunique'
        }).reset_index()
        
        category_agg.columns = [
            'category',
            'transaction_count',
            'total_revenue',
            'avg_revenue',
            'total_quantity',
            'unique_customers'
        ]
        
        category_agg = category_agg.sort_values('total_revenue', ascending=False)
        
        summaries = []
        for _, row in category_agg.iterrows():
            summary = {
                'category': row['category'],
                'transaction_count': int(row['transaction_count']),
                'total_revenue': float(row['total_revenue']),
                'avg_revenue': float(row['avg_revenue']),
                'total_quantity': int(row['total_quantity']),
                'unique_customers': int(row['unique_customers'])
            }
            summaries.append(summary)
            
            logger.info(f"  {summary['category']}: ${summary['total_revenue']:,.2f}")
        
        return summaries
    
    def create_customer_segment_summaries(self, df: pd.DataFrame) -> List[Dict]:
        """Create summaries by customer segment"""
        logger.info("Creating customer segment summaries...")
        
        # Check if customer_segment column exists
        if 'customer_segment' not in df.columns:
            logger.warning("customer_segment column not found, skipping segment summaries")
            return []
        
        segment_agg = df.groupby('customer_segment').agg({
            'transaction_id': 'count',
            'revenue': ['sum', 'mean'],
            'customer_id': 'nunique'
        }).reset_index()
        
        segment_agg.columns = [
            'segment',
            'transaction_count',
            'total_revenue',
            'avg_revenue',
            'unique_customers'
        ]
        
        summaries = []
        for _, row in segment_agg.iterrows():
            summary = {
                'segment': row['segment'],
                'transaction_count': int(row['transaction_count']),
                'total_revenue': float(row['total_revenue']),
                'avg_revenue': float(row['avg_revenue']),
                'unique_customers': int(row['unique_customers']),
                'avg_transactions_per_customer': float(row['transaction_count'] / row['unique_customers'])
            }
            summaries.append(summary)
            
            logger.info(f"  {summary['segment']}: {summary['unique_customers']:,} customers, "
                       f"${summary['total_revenue']:,.2f}")
        
        return summaries
    
    def create_time_based_summaries(self, df: pd.DataFrame) -> Dict:
        """Create time-based analysis"""
        logger.info("Creating time-based summaries...")
        
        summaries = {}
        
        # By day of week (if available)
        if 'day_name' in df.columns:
            dow_agg = df.groupby('day_name').agg({
                'transaction_id': 'count',
                'revenue': 'sum'
            }).reset_index()
            
            summaries['by_day_of_week'] = [
                {
                    'day': row['day_name'],
                    'transaction_count': int(row['transaction_id']),
                    'total_revenue': float(row['revenue'])
                }
                for _, row in dow_agg.iterrows()
            ]
        
        # By month (if available)
        if 'month_name' in df.columns:
            month_agg = df.groupby('month_name').agg({
                'transaction_id': 'count',
                'revenue': 'sum'
            }).reset_index()
            
            summaries['by_month'] = [
                {
                    'month': row['month_name'],
                    'transaction_count': int(row['transaction_id']),
                    'total_revenue': float(row['revenue'])
                }
                for _, row in month_agg.iterrows()
            ]
        
        # By quarter (if available)
        if 'quarter' in df.columns:
            quarter_agg = df.groupby('quarter').agg({
                'transaction_id': 'count',
                'revenue': 'sum'
            }).reset_index()
            
            summaries['by_quarter'] = [
                {
                    'quarter': f"Q{int(row['quarter'])}",
                    'transaction_count': int(row['transaction_id']),
                    'total_revenue': float(row['revenue'])
                }
                for _, row in quarter_agg.iterrows()
            ]
        
        logger.info(f"Created {len(summaries)} time-based summary types")
        return summaries
    
    def create_revenue_category_summaries(self, df: pd.DataFrame) -> List[Dict]:
        """Create summaries by revenue category"""
        logger.info("Creating revenue category summaries...")
        
        if 'revenue_category' not in df.columns:
            logger.warning("revenue_category column not found, skipping")
            return []
        
        category_agg = df.groupby('revenue_category').agg({
            'transaction_id': 'count',
            'revenue': ['sum', 'mean']
        }).reset_index()
        
        category_agg.columns = ['category', 'transaction_count', 'total_revenue', 'avg_revenue']
        
        summaries = []
        for _, row in category_agg.iterrows():
            summary = {
                'revenue_tier': row['category'],
                'transaction_count': int(row['transaction_count']),
                'total_revenue': float(row['total_revenue']),
                'avg_revenue': float(row['avg_revenue']),
                'percentage_of_total': float((row['transaction_count'] / len(df)) * 100)
            }
            summaries.append(summary)
            
            logger.info(f"  {summary['revenue_tier']}: {summary['transaction_count']:,} txns "
                       f"({summary['percentage_of_total']:.1f}%)")
        
        return summaries
    
    def create_top_customers(self, df: pd.DataFrame, top_n: int = 10) -> List[Dict]:
        """Identify top customers by revenue"""
        logger.info(f"Identifying top {top_n} customers...")
        
        customer_agg = df.groupby('customer_id').agg({
            'transaction_id': 'count',
            'revenue': 'sum',
            'quantity': 'sum'
        }).reset_index()
        
        customer_agg.columns = ['customer_id', 'transaction_count', 'total_revenue', 'total_quantity']
        customer_agg = customer_agg.sort_values('total_revenue', ascending=False).head(top_n)
        
        top_customers = []
        for rank, (_, row) in enumerate(customer_agg.iterrows(), 1):
            customer = {
                'rank': rank,
                'customer_id': row['customer_id'],
                'total_revenue': float(row['total_revenue']),
                'transaction_count': int(row['transaction_count']),
                'total_quantity': int(row['total_quantity']),
                'avg_revenue_per_transaction': float(row['total_revenue'] / row['transaction_count'])
            }
            top_customers.append(customer)
        
        logger.info(f"Top customer: {top_customers[0]['customer_id']} "
                   f"(${top_customers[0]['total_revenue']:,.2f})")
        
        return top_customers
    
    def create_regional_product_matrix(self, df: pd.DataFrame) -> List[Dict]:
        """Create region-product performance matrix"""
        logger.info("Creating regional-product matrix...")
        
        matrix = df.groupby(['region', 'product']).agg({
            'transaction_id': 'count',
            'revenue': 'sum'
        }).reset_index()
        
        matrix.columns = ['region', 'product', 'transaction_count', 'total_revenue']
        
        # Get top 3 products per region
        top_products = []
        for region in df['region'].unique():
            region_data = matrix[matrix['region'] == region].sort_values(
                'total_revenue', ascending=False
            ).head(3)
            
            for rank, (_, row) in enumerate(region_data.iterrows(), 1):
                top_products.append({
                    'region': row['region'],
                    'rank': rank,
                    'product': row['product'],
                    'transaction_count': int(row['transaction_count']),
                    'total_revenue': float(row['total_revenue'])
                })
        
        logger.info(f"Created regional-product matrix with {len(top_products)} entries")
        return top_products
    
    def process_file(self, input_key: str, date_str: str) -> Dict:
        """Main aggregation pipeline"""
        try:
            logger.info(f"Starting aggregation for {input_key}")
            start_time = datetime.now()
            
            # Read transformed data
            df = self.read_from_s3(input_key)
            self.metrics['records_aggregated'] = len(df)
            
            # Create all aggregations
            aggregates = {
                'metadata': {
                    'date': date_str,
                    'generated_at': datetime.now().isoformat(),
                    'source_file': input_key,
                    'record_count': len(df)
                },
                'daily_summary': self.create_daily_summary(df, date_str),
                'regional_summaries': self.create_regional_summaries(df),
                'product_summaries': self.create_product_summaries(df),
                'product_category_summaries': self.create_product_category_summaries(df),
                'customer_segment_summaries': self.create_customer_segment_summaries(df),
                'time_based_summaries': self.create_time_based_summaries(df),
                'revenue_category_summaries': self.create_revenue_category_summaries(df),
                'top_customers': self.create_top_customers(df, top_n=10),
                'regional_product_matrix': self.create_regional_product_matrix(df)
            }
            
            # Write aggregates to S3
            output_key = f"{config.AGGREGATES_ZONE}/daily/{date_str}.json"
            self.write_json_to_s3(aggregates, output_key)
            
            # Also create separate files for easy querying
            self._write_separate_aggregates(aggregates, date_str)
            
            # Calculate metrics
            processing_time = (datetime.now() - start_time).total_seconds()
            self.metrics['aggregation_time'] = processing_time
            
            logger.info(f"Aggregation completed in {processing_time:.2f} seconds")
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Aggregation pipeline failed: {str(e)}")
            raise
    
    def _write_separate_aggregates(self, aggregates: Dict, date_str: str):
        """Write separate aggregate files for specific queries"""
        try:
            # Regional aggregates
            regional_key = f"{config.AGGREGATES_ZONE}/regional/{date_str}.json"
            self.write_json_to_s3({
                'date': date_str,
                'summaries': aggregates['regional_summaries']
            }, regional_key)
            
            # Product aggregates
            product_key = f"{config.AGGREGATES_ZONE}/products/{date_str}.json"
            self.write_json_to_s3({
                'date': date_str,
                'summaries': aggregates['product_summaries']
            }, product_key)
            
            logger.info("Separate aggregate files created")
            
        except Exception as e:
            logger.warning(f"Failed to write separate aggregates: {str(e)}")
    
    def print_metrics_summary(self):
        """Print aggregation metrics"""
        logger.info("\n" + "="*60)
        logger.info("AGGREGATION METRICS SUMMARY")
        logger.info("="*60)
        logger.info(f"Records Aggregated:         {self.metrics['records_aggregated']:,}")
        logger.info(f"Daily Summary:              {'✓' if self.metrics['daily_summary_created'] else '✗'}")
        logger.info(f"Regional Summaries:         {self.metrics['regional_summaries_created']}")
        logger.info(f"Product Summaries:          {self.metrics['product_summaries_created']}")
        logger.info(f"Processing Time:            {self.metrics['aggregation_time']:.2f}s")
        logger.info("="*60 + "\n")


def main():
    """Main execution function"""
    
    # Default to today's date if no argument provided
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Processing date: {date_str}")
    
    # Define S3 key (read from processed-zone)
    input_key = f"{config.PROCESSED_ZONE}/{date_str}.csv"
    
    try:
        # Initialize aggregator
        aggregator = DataAggregator(config.S3_BUCKET)
        
        # Process file
        metrics = aggregator.process_file(input_key, date_str)
        
        # Print summary
        aggregator.print_metrics_summary()
        
        logger.info("✅ Aggregator completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Aggregator failed: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())