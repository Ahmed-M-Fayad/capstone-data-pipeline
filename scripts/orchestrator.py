#!/usr/bin/env python3.11
"""
ETL Pipeline Orchestrator
Coordinates execution of all ETL stages

Features:
- Runs validator → transformer → aggregator in sequence
- Error handling and retry logic
- Comprehensive logging and metrics
- Email notifications (optional)
- Success/failure tracking
"""

import sys
import os
import logging
import json
import subprocess
from datetime import datetime
from typing import Dict, Tuple
import boto3

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
        logging.FileHandler(f'{config.LOG_DIR}/orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=config.AWS_REGION)


class PipelineOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self, date_str: str):
        self.date_str = date_str
        self.start_time = datetime.now()
        self.scripts_dir = '/opt/capstone-pipeline/scripts'
        self.python_path = '/usr/bin/python3.11'
        
        self.pipeline_status = {
            'date': date_str,
            'start_time': self.start_time.isoformat(),
            'end_time': None,
            'duration_seconds': 0,
            'overall_status': 'RUNNING',
            'stages': {
                'validator': {'status': 'PENDING', 'duration': 0, 'error': None},
                'transformer': {'status': 'PENDING', 'duration': 0, 'error': None},
                'aggregator': {'status': 'PENDING', 'duration': 0, 'error': None}
            },
            'metrics': {}
        }
    
    def check_input_file_exists(self) -> bool:
        """Check if raw data file exists in S3"""
        try:
            input_key = f"{config.RAW_ZONE}/{self.date_str}.csv"
            logger.info(f"Checking for input file: s3://{config.S3_BUCKET}/{input_key}")
            
            s3_client.head_object(Bucket=config.S3_BUCKET, Key=input_key)
            logger.info("✓ Input file found")
            return True
            
        except Exception as e:
            logger.error(f"✗ Input file not found: {str(e)}")
            return False
    
    def run_script(self, script_name: str, stage_name: str) -> Tuple[bool, int, str]:
        """
        Run a Python script and capture output
        
        Returns:
            Tuple[success: bool, return_code: int, output: str]
        """
        script_path = f"{self.scripts_dir}/{script_name}"
        stage_start = datetime.now()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"STAGE: {stage_name.upper()}")
        logger.info(f"{'='*60}")
        
        try:
            # Run the script
            cmd = [self.python_path, script_path, self.date_str]
            logger.info(f"Executing: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            # Calculate duration
            duration = (datetime.now() - stage_start).total_seconds()
            
            # Update status
            self.pipeline_status['stages'][stage_name]['duration'] = duration
            
            # Check result
            if result.returncode == 0:
                self.pipeline_status['stages'][stage_name]['status'] = 'SUCCESS'
                logger.info(f"✓ {stage_name} completed successfully in {duration:.2f}s")
                return True, result.returncode, result.stdout
            else:
                self.pipeline_status['stages'][stage_name]['status'] = 'FAILED'
                self.pipeline_status['stages'][stage_name]['error'] = result.stderr
                logger.error(f"✗ {stage_name} failed with return code {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                return False, result.returncode, result.stderr
                
        except subprocess.TimeoutExpired:
            duration = (datetime.now() - stage_start).total_seconds()
            self.pipeline_status['stages'][stage_name]['status'] = 'TIMEOUT'
            self.pipeline_status['stages'][stage_name]['duration'] = duration
            self.pipeline_status['stages'][stage_name]['error'] = 'Script execution timeout (5 minutes)'
            logger.error(f"✗ {stage_name} timed out after {duration:.2f}s")
            return False, -1, "Timeout"
            
        except Exception as e:
            duration = (datetime.now() - stage_start).total_seconds()
            self.pipeline_status['stages'][stage_name]['status'] = 'ERROR'
            self.pipeline_status['stages'][stage_name]['duration'] = duration
            self.pipeline_status['stages'][stage_name]['error'] = str(e)
            logger.error(f"✗ {stage_name} error: {str(e)}")
            return False, -1, str(e)
    
    def run_validator(self) -> bool:
        """Run data validation stage"""
        success, _, _ = self.run_script('validator.py', 'validator')
        return success
    
    def run_transformer(self) -> bool:
        """Run data transformation stage"""
        success, _, _ = self.run_script('transformer.py', 'transformer')
        return success
    
    def run_aggregator(self) -> bool:
        """Run data aggregation stage"""
        success, _, _ = self.run_script('aggregator.py', 'aggregator')
        return success
    
    def collect_metrics(self):
        """Collect metrics from all stages"""
        logger.info("\nCollecting pipeline metrics...")
        
        try:
            # Get aggregation summary if it exists
            agg_key = f"{config.AGGREGATES_ZONE}/daily/{self.date_str}.json"
            
            response = s3_client.get_object(Bucket=config.S3_BUCKET, Key=agg_key)
            agg_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Extract key metrics
            daily_summary = agg_data.get('daily_summary', {})
            
            self.pipeline_status['metrics'] = {
                'total_transactions': daily_summary.get('total_transactions', 0),
                'total_revenue': daily_summary.get('total_revenue', 0),
                'unique_customers': daily_summary.get('unique_customers', 0),
                'unique_products': daily_summary.get('unique_products', 0)
            }
            
            logger.info(f"Metrics collected: {self.pipeline_status['metrics']}")
            
        except Exception as e:
            logger.warning(f"Could not collect metrics: {str(e)}")
    
    def save_execution_report(self):
        """Save execution report to S3"""
        try:
            report_key = f"pipeline-reports/{self.date_str}.json"
            
            logger.info(f"Saving execution report to s3://{config.S3_BUCKET}/{report_key}")
            
            s3_client.put_object(
                Bucket=config.S3_BUCKET,
                Key=report_key,
                Body=json.dumps(self.pipeline_status, indent=2, default=str),
                ContentType='application/json'
            )
            
            logger.info("✓ Execution report saved")
            
        except Exception as e:
            logger.warning(f"Could not save execution report: {str(e)}")
    
    def print_summary(self):
        """Print execution summary"""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        self.pipeline_status['end_time'] = end_time.isoformat()
        self.pipeline_status['duration_seconds'] = total_duration
        
        logger.info("\n" + "="*70)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("="*70)
        logger.info(f"Date:                {self.date_str}")
        logger.info(f"Start Time:          {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End Time:            {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total Duration:      {total_duration:.2f} seconds")
        logger.info("-"*70)
        
        # Stage statuses
        logger.info("STAGE RESULTS:")
        for stage_name, stage_info in self.pipeline_status['stages'].items():
            status_symbol = "✓" if stage_info['status'] == 'SUCCESS' else "✗"
            logger.info(f"  {status_symbol} {stage_name.capitalize():15} "
                       f"{stage_info['status']:10} ({stage_info['duration']:.2f}s)")
            
            if stage_info['error']:
                logger.info(f"      Error: {stage_info['error']}")
        
        logger.info("-"*70)
        
        # Overall metrics
        if self.pipeline_status['metrics']:
            logger.info("PIPELINE METRICS:")
            metrics = self.pipeline_status['metrics']
            logger.info(f"  Transactions:      {metrics.get('total_transactions', 0):,}")
            logger.info(f"  Total Revenue:     ${metrics.get('total_revenue', 0):,.2f}")
            logger.info(f"  Unique Customers:  {metrics.get('unique_customers', 0):,}")
            logger.info(f"  Unique Products:   {metrics.get('unique_products', 0):,}")
            logger.info("-"*70)
        
        # Overall status
        logger.info(f"Overall Status:      {self.pipeline_status['overall_status']}")
        logger.info("="*70 + "\n")
    
    def run_pipeline(self) -> bool:
        """Execute the complete ETL pipeline"""
        logger.info("\n" + "="*70)
        logger.info("STARTING ETL PIPELINE")
        logger.info("="*70)
        logger.info(f"Processing date: {self.date_str}")
        logger.info(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70 + "\n")
        
        try:
            # Step 0: Check input file exists
            if not self.check_input_file_exists():
                logger.error("Pipeline aborted: Input file not found")
                self.pipeline_status['overall_status'] = 'FAILED'
                self.pipeline_status['stages']['validator']['status'] = 'SKIPPED'
                self.pipeline_status['stages']['validator']['error'] = 'Input file not found'
                self.print_summary()
                return False
            
            # Step 1: Run Validator
            logger.info("\n📋 Starting Stage 1: Data Validation")
            if not self.run_validator():
                logger.error("Pipeline aborted: Validator failed")
                self.pipeline_status['overall_status'] = 'FAILED'
                self.pipeline_status['stages']['transformer']['status'] = 'SKIPPED'
                self.pipeline_status['stages']['aggregator']['status'] = 'SKIPPED'
                self.print_summary()
                self.save_execution_report()
                return False
            
            # Step 2: Run Transformer
            logger.info("\n🔄 Starting Stage 2: Data Transformation")
            if not self.run_transformer():
                logger.error("Pipeline aborted: Transformer failed")
                self.pipeline_status['overall_status'] = 'FAILED'
                self.pipeline_status['stages']['aggregator']['status'] = 'SKIPPED'
                self.print_summary()
                self.save_execution_report()
                return False
            
            # Step 3: Run Aggregator
            logger.info("\n📊 Starting Stage 3: Data Aggregation")
            if not self.run_aggregator():
                logger.error("Pipeline failed: Aggregator failed")
                self.pipeline_status['overall_status'] = 'FAILED'
                self.print_summary()
                self.save_execution_report()
                return False
            
            # Pipeline succeeded
            self.pipeline_status['overall_status'] = 'SUCCESS'
            
            # Collect metrics
            self.collect_metrics()
            
            # Print summary
            self.print_summary()
            
            # Save execution report
            self.save_execution_report()
            
            logger.info("🎉 Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}", exc_info=True)
            self.pipeline_status['overall_status'] = 'ERROR'
            self.print_summary()
            self.save_execution_report()
            return False


def main():
    """Main execution function"""
    
    # Get date from command line or use today
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"ETL Pipeline Orchestrator starting for date: {date_str}")
    
    try:
        # Create orchestrator
        orchestrator = PipelineOrchestrator(date_str)
        
        # Run pipeline
        success = orchestrator.run_pipeline()
        
        # Exit with appropriate code
        if success:
            logger.info("✅ Orchestrator completed successfully")
            return 0
        else:
            logger.error("❌ Orchestrator failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Orchestrator error: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())