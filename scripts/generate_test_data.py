#!/usr/bin/env python3
"""
Test Data Generator for Capstone Data Pipeline
Generates realistic sales transaction data for testing ETL pipeline
"""

import csv
import random
import argparse
from datetime import datetime, timedelta


def generate_sales_data(num_records, output_file, date_str=None):
    """
    Generate realistic sales transaction data

    Args:
        num_records: Number of records to generate
        output_file: Output CSV filename
        date_str: Optional date (YYYY-MM-DD), defaults to today
    """

    # Configuration
    regions = ["North", "South", "East", "West", "Central"]
    products = [
        "Laptop",
        "Desktop",
        "Monitor",
        "Keyboard",
        "Mouse",
        "Headset",
        "Webcam",
        "Router",
        "Switch",
        "Cable",
        "USB Drive",
        "External HDD",
        "SSD",
        "RAM",
        "Motherboard",
    ]

    # Date handling
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        target_date = datetime.now()

    print(f"🔄 Generating {num_records:,} sales records for {target_date.date()}...")
    print(f"📁 Output file: {output_file}")

    records_written = 0
    duplicates_added = 0
    nulls_added = 0

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)

        # Write header
        writer.writerow(
            [
                "transaction_id",
                "date",
                "region",
                "product",
                "quantity",
                "price",
                "customer_id",
            ]
        )

        # Generate records
        for i in range(num_records):
            # Add some randomness to date (within same day)
            record_date = target_date + timedelta(
                hours=random.randint(0, 23), minutes=random.randint(0, 59)
            )

            # Generate base record
            transaction_id = f"TXN{i:08d}"
            region = random.choice(regions)
            product = random.choice(products)
            quantity = random.randint(1, 50)
            price = round(random.uniform(10.99, 999.99), 2)
            customer_id = f"CUST{random.randint(1000, 9999)}"

            # Intentionally add some data quality issues for validation testing
            # ~2% duplicates
            if random.random() < 0.02 and i > 100:
                transaction_id = f"TXN{random.randint(0, i-1):08d}"
                duplicates_added += 1

            # ~1% null values in non-critical fields
            if random.random() < 0.01:
                customer_id = ""
                nulls_added += 1

            # Write record
            writer.writerow(
                [
                    transaction_id,
                    record_date.strftime("%Y-%m-%d %H:%M:%S"),
                    region,
                    product,
                    quantity,
                    price,
                    customer_id,
                ]
            )

            records_written += 1

            # Progress indicator
            if (i + 1) % 10000 == 0:
                print(f"  ✓ Generated {i + 1:,} records...")

    # Summary
    print(f"\n✅ Generation Complete!")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"📊 Total records:     {records_written:,}")
    print(
        f"🔄 Duplicates added:  {duplicates_added:,} (~{duplicates_added/records_written*100:.1f}%)"
    )
    print(
        f"⚠️  Null values added: {nulls_added:,} (~{nulls_added/records_written*100:.1f}%)"
    )
    print(f"📁 File size:         {get_file_size(output_file)}")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"\n💡 These quality issues are intentional for testing the validator!")


def get_file_size(filename):
    """Get human-readable file size"""
    import os

    size_bytes = os.path.getsize(filename)

    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate test sales data for capstone pipeline"
    )
    parser.add_argument(
        "--records",
        type=int,
        default=125000,
        help="Number of records to generate (default: 125000)",
    )
    parser.add_argument(
        "--output",
        default="test_sales.csv",
        help="Output CSV filename (default: test_sales.csv)",
    )
    parser.add_argument(
        "--date", default=None, help="Date for records (YYYY-MM-DD, default: today)"
    )

    args = parser.parse_args()

    generate_sales_data(args.records, args.output, args.date)

    print(f"\n🚀 Ready to upload to S3!")
    print(f"   Next step: Upload {args.output} to S3 raw-zone folder")
