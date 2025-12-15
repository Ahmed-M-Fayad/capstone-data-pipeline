#!/usr/bin/env python3.11
"""
Enhanced Dashboard Generator
Creates beautiful HTML dashboard with embedded data and advanced metrics
"""

import sys
import os
import logging
import json
from datetime import datetime, timedelta
from typing import Dict
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
        logging.FileHandler(f'{config.LOG_DIR}/dashboard_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=config.AWS_REGION)


class DashboardGenerator:
    """Generates enhanced HTML dashboard with embedded data"""
    
    def __init__(self, data_bucket: str, dashboard_bucket: str):
        self.data_bucket = data_bucket
        self.dashboard_bucket = dashboard_bucket
    
    def read_aggregates(self, date_str: str) -> Dict:
        """Read aggregated data from S3"""
        try:
            agg_key = f"{config.AGGREGATES_ZONE}/daily/{date_str}.json"
            logger.info(f"Reading aggregates: s3://{self.data_bucket}/{agg_key}")
            
            response = s3_client.get_object(Bucket=self.data_bucket, Key=agg_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            logger.info(f"Successfully read aggregates for {date_str}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to read aggregates: {str(e)}")
            raise
    
    def format_large_number(self, num: float) -> str:
        """Format large numbers with K/M suffixes"""
        if num >= 1_000_000:
            return f"${num/1_000_000:.2f}M"
        elif num >= 1_000:
            return f"${num/1_000:.1f}K"
        else:
            return f"${num:.2f}"
    
    def generate_html(self, date_str: str, data: Dict) -> str:
        """Generate enhanced HTML with embedded data"""
        
        daily = data.get('daily_summary', {})
        regions = data.get('regional_summaries', [])
        top_products = data.get('product_summaries', [])[:5]  # Top 5 products
        
        # Sort regions by revenue
        regions_sorted = sorted(regions, key=lambda x: x['total_revenue'], reverse=True)
        max_revenue = max([r['total_revenue'] for r in regions_sorted]) if regions_sorted else 1
        
        # Calculate additional insights
        total_revenue = daily.get('total_revenue', 0)
        total_transactions = daily.get('total_transactions', 0)
        avg_revenue = daily.get('average_revenue', 0)
        median_revenue = daily.get('median_revenue', 0)
        
        # Build regional HTML with better formatting
        regional_html = ""
        for i, region in enumerate(regions_sorted):
            percentage = (region['total_revenue'] / total_revenue * 100) if total_revenue > 0 else 0
            bar_width = (region['total_revenue'] / max_revenue * 100)
            
            # Rank badge color
            rank_colors = ['#FFD700', '#C0C0C0', '#CD7F32', '#667eea', '#764ba2']
            rank_color = rank_colors[i] if i < len(rank_colors) else '#999'
            
            regional_html += f"""
                <div class="region-item">
                    <div class="region-rank" style="background: {rank_color};">#{i+1}</div>
                    <div class="region-name">{region['region']}</div>
                    <div class="region-bar">
                        <div class="region-bar-fill" style="width: {bar_width:.1f}%">
                            <span class="bar-percentage">{percentage:.1f}%</span>
                        </div>
                    </div>
                    <div class="region-stats">
                        <div class="stat-revenue">{self.format_large_number(region['total_revenue'])}</div>
                        <div class="stat-transactions">{region['transaction_count']:,} orders</div>
                    </div>
                </div>
            """
        
        # Build top products HTML
        products_html = ""
        if top_products:
            for i, product in enumerate(top_products):
                products_html += f"""
                    <div class="product-item">
                        <div class="product-rank">#{i+1}</div>
                        <div class="product-name">{product.get('product', 'Unknown')}</div>
                        <div class="product-sales">{product.get('total_quantity', 0):,} units</div>
                        <div class="product-revenue">{self.format_large_number(product.get('total_revenue', 0))}</div>
                    </div>
                """
        else:
            products_html = '<div class="no-data">Product data not available</div>'
        
        # Complete HTML template
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sales Pipeline Dashboard - {date_str}</title>
    <meta http-equiv="refresh" content="300">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            line-height: 1.6;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        /* Header */
        header {{
            background: white;
            padding: 40px;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            margin-bottom: 30px;
            text-align: center;
        }}
        
        h1 {{
            color: #1a202c;
            font-size: 2.5em;
            font-weight: 700;
            margin-bottom: 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        .subtitle {{
            color: #718096;
            font-size: 1.1em;
            font-weight: 400;
        }}
        
        .date-badge {{
            display: inline-block;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 10px 25px;
            border-radius: 25px;
            margin-top: 15px;
            font-size: 0.95em;
            font-weight: 600;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        }}
        
        /* Metrics Grid */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 25px;
            margin-bottom: 30px;
        }}
        
        .metric-card {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }}
        
        .metric-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
            background: linear-gradient(90deg, #667eea, #764ba2);
        }}
        
        .metric-card:hover {{
            transform: translateY(-8px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.15);
        }}
        
        .metric-label {{
            color: #718096;
            font-size: 0.85em;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            margin-bottom: 12px;
            font-weight: 600;
        }}
        
        .metric-value {{
            color: #1a202c;
            font-size: 2.2em;
            font-weight: 700;
            margin-bottom: 8px;
            word-break: break-word;
        }}
        
        .metric-subtext {{
            color: #a0aec0;
            font-size: 0.9em;
            font-weight: 400;
        }}
        
        .metric-icon {{
            position: absolute;
            top: 20px;
            right: 20px;
            font-size: 2.5em;
            opacity: 0.1;
        }}
        
        /* Sections */
        .section {{
            background: white;
            padding: 35px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        
        .section h2 {{
            color: #1a202c;
            margin-bottom: 25px;
            font-size: 1.5em;
            font-weight: 700;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        
        /* Regional Performance */
        .region-list {{
            display: grid;
            gap: 18px;
        }}
        
        .region-item {{
            display: grid;
            grid-template-columns: 50px 120px 1fr 180px;
            gap: 20px;
            align-items: center;
            padding: 20px;
            background: linear-gradient(135deg, #f7fafc 0%, #edf2f7 100%);
            border-radius: 12px;
            transition: all 0.3s ease;
        }}
        
        .region-item:hover {{
            background: linear-gradient(135deg, #edf2f7 0%, #e2e8f0 100%);
            transform: translateX(5px);
        }}
        
        .region-rank {{
            width: 40px;
            height: 40px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: 700;
            font-size: 1.1em;
            box-shadow: 0 4px 10px rgba(0,0,0,0.2);
        }}
        
        .region-name {{
            font-weight: 600;
            color: #2d3748;
            font-size: 1.1em;
        }}
        
        .region-bar {{
            background: #e2e8f0;
            height: 32px;
            border-radius: 20px;
            overflow: hidden;
            position: relative;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .region-bar-fill {{
            background: linear-gradient(90deg, #667eea, #764ba2);
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding: 0 15px;
            transition: width 1.5s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3);
        }}
        
        .bar-percentage {{
            color: white;
            font-size: 0.85em;
            font-weight: 700;
            text-shadow: 0 1px 2px rgba(0,0,0,0.2);
        }}
        
        .region-stats {{
            text-align: right;
        }}
        
        .stat-revenue {{
            font-weight: 700;
            color: #2d3748;
            font-size: 1.2em;
            margin-bottom: 3px;
        }}
        
        .stat-transactions {{
            color: #718096;
            font-size: 0.9em;
        }}
        
        /* Top Products */
        .product-list {{
            display: grid;
            gap: 12px;
        }}
        
        .product-item {{
            display: grid;
            grid-template-columns: 50px 1fr 150px 150px;
            gap: 20px;
            align-items: center;
            padding: 18px 22px;
            background: #f7fafc;
            border-radius: 10px;
            border-left: 4px solid #667eea;
            transition: all 0.3s ease;
        }}
        
        .product-item:hover {{
            background: #edf2f7;
            border-left-width: 6px;
            transform: translateX(3px);
        }}
        
        .product-rank {{
            color: #667eea;
            font-weight: 700;
            font-size: 1.3em;
        }}
        
        .product-name {{
            color: #2d3748;
            font-weight: 600;
        }}
        
        .product-sales {{
            color: #718096;
            font-size: 0.95em;
            text-align: right;
        }}
        
        .product-revenue {{
            color: #2d3748;
            font-weight: 700;
            font-size: 1.1em;
            text-align: right;
        }}
        
        .no-data {{
            text-align: center;
            color: #a0aec0;
            padding: 30px;
            font-style: italic;
        }}
        
        /* Footer */
        footer {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            text-align: center;
            color: #718096;
        }}
        
        footer strong {{
            color: #2d3748;
        }}
        
        .last-updated {{
            font-size: 0.9em;
            margin-top: 12px;
            color: #a0aec0;
        }}
        
        .auto-refresh {{
            background: linear-gradient(135deg, #48bb78, #38a169);
            color: white;
            padding: 6px 18px;
            border-radius: 20px;
            font-size: 0.85em;
            display: inline-block;
            margin-top: 12px;
            font-weight: 600;
            box-shadow: 0 4px 10px rgba(72, 187, 120, 0.3);
        }}
        
        /* Responsive Design */
        @media (max-width: 1024px) {{
            .region-item {{
                grid-template-columns: 50px 100px 1fr 150px;
                gap: 15px;
            }}
            
            .product-item {{
                grid-template-columns: 40px 1fr 120px 120px;
                gap: 15px;
            }}
        }}
        
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
            
            h1 {{
                font-size: 1.8em;
            }}
            
            .metrics-grid {{
                grid-template-columns: 1fr;
            }}
            
            .metric-value {{
                font-size: 1.8em;
            }}
            
            .region-item {{
                grid-template-columns: 1fr;
                gap: 12px;
            }}
            
            .region-bar {{
                grid-column: 1 / -1;
            }}
            
            .region-stats {{
                text-align: left;
            }}
            
            .product-item {{
                grid-template-columns: 1fr;
                gap: 8px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 Sales Pipeline Dashboard</h1>
            <p class="subtitle">Real-time AWS Data Pipeline Analytics</p>
            <div class="date-badge">📅 {date_str}</div>
        </header>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-icon">💰</div>
                <div class="metric-label">Total Revenue</div>
                <div class="metric-value">{self.format_large_number(total_revenue)}</div>
                <div class="metric-subtext">Daily earnings</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">🛒</div>
                <div class="metric-label">Transactions</div>
                <div class="metric-value">{total_transactions:,}</div>
                <div class="metric-subtext">Orders processed</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">📈</div>
                <div class="metric-label">Avg Order Value</div>
                <div class="metric-value">${avg_revenue:,.2f}</div>
                <div class="metric-subtext">Median: ${median_revenue:,.2f}</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">👥</div>
                <div class="metric-label">Customers</div>
                <div class="metric-value">{daily.get('unique_customers', 0):,}</div>
                <div class="metric-subtext">{daily.get('unique_products', 0):,} products</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">📦</div>
                <div class="metric-label">Items Sold</div>
                <div class="metric-value">{daily.get('total_quantity_sold', 0):,}</div>
                <div class="metric-subtext">Units shipped</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">🎯</div>
                <div class="metric-label">Conversion Rate</div>
                <div class="metric-value">{(total_transactions / daily.get('unique_customers', 1) * 100):.1f}%</div>
                <div class="metric-subtext">Orders per customer</div>
            </div>
        </div>
        
        <div class="section">
            <h2>📍 Regional Performance</h2>
            <div class="region-list">
                {regional_html}
            </div>
        </div>
        
        <div class="section">
            <h2>🏆 Top Products</h2>
            <div class="product-list">
                {products_html}
            </div>
        </div>
        
        <footer>
            <p><strong>Architecture:</strong> S3 Data Lake → EC2 ETL Pipeline → Python Processing</p>
            <p class="last-updated">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            <p class="auto-refresh">🔄 Auto-refresh every 5 minutes</p>
        </footer>
    </div>
</body>
</html>"""
        
        return html
    
    def upload_dashboard(self, html_content: str) -> bool:
        """Upload dashboard to S3"""
        try:
            logger.info(f"Uploading dashboard to s3://{self.dashboard_bucket}/index.html")
            
            s3_client.put_object(
                Bucket=self.dashboard_bucket,
                Key='index.html',
                Body=html_content.encode('utf-8'),
                ContentType='text/html',
                CacheControl='max-age=60'
            )
            
            logger.info("✓ Dashboard uploaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload dashboard: {str(e)}")
            return False
    
    def generate_for_date(self, date_str: str) -> bool:
        """Generate and upload dashboard for specific date"""
        try:
            logger.info(f"\nGenerating enhanced dashboard for {date_str}")
            
            # Read aggregates
            data = self.read_aggregates(date_str)
            
            # Generate HTML
            html = self.generate_html(date_str, data)
            
            # Upload to S3
            success = self.upload_dashboard(html)
            
            if success:
                url = f"http://{self.dashboard_bucket}.s3-website-{config.AWS_REGION}.amazonaws.com"
                logger.info(f"\n✅ Enhanced dashboard available at: {url}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to generate dashboard: {str(e)}")
            return False


def main():
    """Main execution function"""
    
    # Get date from command line or use today
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Enhanced Dashboard Generator starting for date: {date_str}")
    
    try:
        # Initialize generator
        generator = DashboardGenerator(
            data_bucket=config.S3_BUCKET,
            dashboard_bucket='capstone-dashboard-590183856719'
        )
        
        # Generate dashboard
        success = generator.generate_for_date(date_str)
        
        if success:
            logger.info("✅ Enhanced dashboard generation completed successfully")
            return 0
        else:
            logger.error("❌ Dashboard generation failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Dashboard Generator error: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())