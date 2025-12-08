import boto3
import sys

def test_s3_connection():
    try:
        # Create S3 client
        s3 = boto3.client('s3')
        
        # List buckets
        print("✅ Testing S3 connection...")
        response = s3.list_buckets()
        
        print(f"\n📦 Found {len(response['Buckets'])} buckets:")
        for bucket in response['Buckets']:
            print(f"  • {bucket['Name']}")
        
        # Test reading from your data lake
        bucket_name = 'capstone-datalake-590183856719'
        print(f"\n📂 Contents of {bucket_name}:")
        
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=10)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"  • {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("  (empty)")
        
        print("\n✅ S3 access test PASSED!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

if __name__ == '__main__':
    success = test_s3_connection()
    sys.exit(0 if success else 1)