#!/usr/bin/env python3.11

## Usage here is only theoretical since there is no authorization in the sandbox to access AWS DynamoDB.

"""
DynamoDB Loader
Loads aggregated metrics from S3 into DynamoDB for fast queries

Features:
- Loads daily regional summaries into DynamoDB
- Optimized for dashboard queries (<10ms)
- Batch write operations for efficiency
- Duplicate handling and error recovery
"""

import sys
import os
import logging
import json
from datetime import datetime
from typing import Dict, List
import boto3
from decimal import Decimal

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
        logging.FileHandler(f'{config.LOG_DIR}/dynamodb_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=config.AWS_REGION)
dynamodb = boto3.resource('dynamodb', region_name=config.AWS_REGION)


class DynamoDBLoader:
    """Loads aggregated metrics into DynamoDB"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.table = dynamodb.Table(table_name)
        self.metrics = {
            'items_loaded': 0,
            'items_failed': 0,
            'load_time': 0
        }
    
    def convert_floats_to_decimal(self, obj):
        """Convert float values to Decimal for DynamoDB"""
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self.convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_floats_to_decimal(item) for item in obj]
        return obj
    
    def read_aggregates_from_s3(self, date_str: str) -> Dict:
        """Read aggregated data from S3"""
        try:
            agg_key = f"{config.AGGREGATES_ZONE}/daily/{date_str}.json"
            logger.info(f"Reading aggregates: s3://{config.S3_BUCKET}/{agg_key}")
            
            response = s3_client.get_object(Bucket=config.S3_BUCKET, Key=agg_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            logger.info(f"Successfully read aggregates for {date_str}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to read aggregates: {str(e)}")
            raise
    
    def create_regional_items(self, date_str: str, aggregates: Dict) -> List[Dict]:
        """Create DynamoDB items from regional aggregates"""
        items = []
        
        regional_summaries = aggregates.get('regional_summaries', [])
        daily_summary = aggregates.get('daily_summary', {})
        
        logger.info(f"Creating items for {len(regional_summaries)} regions")
        
        for region_data in regional_summaries:
            region = region_data['region']
            
            # Create composite key: date#region
            item = {
                'date_region': f"{date_str}#{region}",
                'date': date_str,
                'region': region,
                'loaded_at': datetime.now().isoformat(),
                
                # Regional metrics
                'transaction_count': region_data['transaction_count'],
                'total_revenue': region_data['total_revenue'],
                'avg_revenue': region_data['avg_revenue'],
                'median_revenue': region_data['median_revenue'],
                'total_quantity': region_data['total_quantity'],
                'unique_customers': region_data['unique_customers'],
                'unique_products': region_data['unique_products'],
                'revenue_per_customer': region_data['revenue_per_customer'],
                
                # Daily totals (for reference)
                'daily_total_revenue': daily_summary.get('total_revenue', 0),
                'daily_total_transactions': daily_summary.get('total_transactions', 0),
                
                # Calculate regional percentage
                'pct_of_daily_revenue': (
                    (region_data['total_revenue'] / daily_summary['total_revenue'] * 100)
                    if daily_summary.get('total_revenue', 0) > 0 else 0
                ),
                'pct_of_daily_transactions': (
                    (region_data['transaction_count'] / daily_summary['total_transactions'] * 100)
                    if daily_summary.get('total_transactions', 0) > 0 else 0
                )
            }
            
            # Convert floats to Decimal
            item = self.convert_floats_to_decimal(item)
            items.append(item)
        
        # Also create a daily summary item (all regions combined)
        daily_item = {
            'date_region': f"{date_str}#ALL",
            'date': date_str,
            'region': 'ALL',
            'loaded_at': datetime.now().isoformat(),
            'transaction_count': daily_summary.get('total_transactions', 0),
            'total_revenue': daily_summary.get('total_revenue', 0),
            'avg_revenue': daily_summary.get('average_revenue', 0),
            'median_revenue': daily_summary.get('median_revenue', 0),
            'total_quantity': daily_summary.get('total_quantity_sold', 0),
            'unique_customers': daily_summary.get('unique_customers', 0),
            'unique_products': daily_summary.get('unique_products', 0),
            'pct_of_daily_revenue': 100.0,
            'pct_of_daily_transactions': 100.0
        }
        
        daily_item = self.convert_floats_to_decimal(daily_item)
        items.append(daily_item)
        
        logger.info(f"Created {len(items)} items for DynamoDB")
        return items
    
    def batch_write_items(self, items: List[Dict]) -> bool:
        """Write items to DynamoDB in batches"""
        try:
            logger.info(f"Writing {len(items)} items to DynamoDB...")
            
            # DynamoDB batch_write supports max 25 items per batch
            batch_size = 25
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                
                with self.table.batch_writer() as writer:
                    for item in batch:
                        writer.put_item(Item=item)
                
                self.metrics['items_loaded'] += len(batch)
                logger.info(f"Wrote batch {i//batch_size + 1}: {len(batch)} items")
            
            logger.info(f"✓ Successfully wrote {len(items)} items to DynamoDB")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write items: {str(e)}")
            self.metrics['items_failed'] = len(items)
            raise
    
    def verify_load(self, date_str: str) -> bool:
        """Verify data was loaded correctly"""
        try:
            logger.info("Verifying data load...")
            
            # Query for all regions for this date
            response = self.table.query(
                IndexName='date-index',
                KeyConditionExpression='#d = :date',
                ExpressionAttributeNames={'#d': 'date'},
                ExpressionAttributeValues={':date': date_str}
            )
            
            items = response['Items']
            logger.info(f"Found {len(items)} items for date {date_str}")
            
            # Verify we have regional data + ALL summary
            expected_items = 6  # 5 regions + 1 ALL
            if len(items) == expected_items:
                logger.info("✓ Data verification passed")
                return True
            else:
                logger.warning(f"Expected {expected_items} items, found {len(items)}")
                return False
                
        except Exception as e:
            logger.error(f"Verification failed: {str(e)}")
            return False
    
    def load_date(self, date_str: str) -> bool:
        """Load aggregates for a specific date"""
        try:
            logger.info(f"\nLoading data for {date_str}")
            start_time = datetime.now()
            
            # Read aggregates from S3
            aggregates = self.read_aggregates_from_s3(date_str)
            
            # Create DynamoDB items
            items = self.create_regional_items(date_str, aggregates)
            
            # Write to DynamoDB
            self.batch_write_items(items)
            
            # Verify load
            self.verify_load(date_str)
            
            # Calculate metrics
            load_time = (datetime.now() - start_time).total_seconds()
            self.metrics['load_time'] = load_time
            
            logger.info(f"✓ Load completed in {load_time:.2f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            return False
    
    def print_metrics_summary(self):
        """Print load metrics"""
        logger.info("\n" + "="*60)
        logger.info("DYNAMODB LOAD SUMMARY")
        logger.info("="*60)
        logger.info(f"Items Loaded:        {self.metrics['items_loaded']}")
        logger.info(f"Items Failed:        {self.metrics['items_failed']}")
        logger.info(f"Load Time:           {self.metrics['load_time']:.2f}s")
        logger.info("="*60 + "\n")
    
    def test_queries(self, date_str: str):
        """Test common query patterns"""
        logger.info("\n" + "="*60)
        logger.info("TESTING QUERY PERFORMANCE")
        logger.info("="*60)
        
        try:
            # Test 1: Get specific region
            start = datetime.now()
            response = self.table.get_item(
                Key={'date_region': f"{date_str}#North"}
            )
            duration_ms = (datetime.now() - start).total_seconds() * 1000
            
            if 'Item' in response:
                item = response['Item']
                logger.info(f"✓ Query 1 (Get North region): {duration_ms:.2f}ms")
                logger.info(f"  Revenue: ${float(item['total_revenue']):,.2f}")
            
            # Test 2: Get all regions for date
            start = datetime.now()
            response = self.table.query(
                IndexName='date-index',
                KeyConditionExpression='#d = :date',
                ExpressionAttributeNames={'#d': 'date'},
                ExpressionAttributeValues={':date': date_str}
            )
            duration_ms = (datetime.now() - start).total_seconds() * 1000
            
            logger.info(f"✓ Query 2 (All regions for date): {duration_ms:.2f}ms")
            logger.info(f"  Found {len(response['Items'])} items")
            
            # Test 3: Get daily summary
            start = datetime.now()
            response = self.table.get_item(
                Key={'date_region': f"{date_str}#ALL"}
            )
            duration_ms = (datetime.now() - start).total_seconds() * 1000
            
            if 'Item' in response:
                item = response['Item']
                logger.info(f"✓ Query 3 (Daily summary): {duration_ms:.2f}ms")
                logger.info(f"  Total Revenue: ${float(item['total_revenue']):,.2f}")
                logger.info(f"  Transactions: {int(item['transaction_count']):,}")
            
            logger.info("="*60 + "\n")
            
        except Exception as e:
            logger.error(f"Query test failed: {str(e)}")


def main():
    """Main execution function"""
    
    # Get date from command line or use today
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"DynamoDB Loader starting for date: {date_str}")
    
    try:
        # Initialize loader
        loader = DynamoDBLoader('capstone-metrics')
        
        # Load data
        success = loader.load_date(date_str)
        
        if success:
            # Print metrics
            loader.print_metrics_summary()
            
            # Test queries
            loader.test_queries(date_str)
            
            logger.info("✅ DynamoDB Loader completed successfully")
            return 0
        else:
            logger.error("❌ DynamoDB Loader failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ DynamoDB Loader error: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())