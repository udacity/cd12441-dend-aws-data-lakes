"""
Setup verification for Lesson 2 - Exercise 1: Athena Performance Analysis

Verifies that Lesson 1 data exists and sets up Athena output location.
"""

import boto3
import os

BUCKET_NAME = os.environ.get('BUCKET_NAME')
REGION = 'us-east-1'

s3 = boto3.client('s3', region_name=REGION)

def verify_lesson1_data():
    """Verify Lesson 1 data exists"""
    print("\n[Step 1] Verifying Lesson 1 Data...")
    
    required_paths = {
        'bronze/orders/': 'Lesson 1 - Exercise 1 (Structured)',
        'bronze/structured/orders/raw/': 'Lesson 1 - Exercise 3 (Organized)'
    }
    
    all_exist = True
    for path, description in required_paths.items():
        try:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=path, MaxKeys=1)
            if response.get('KeyCount', 0) > 0:
                print(f"  ✓ {description}")
                print(f"    s3://{BUCKET_NAME}/{path}")
            else:
                print(f"  ✗ {description} - No data found")
                print(f"    Expected: s3://{BUCKET_NAME}/{path}")
                all_exist = False
        except Exception as e:
            print(f"  ✗ {description} - Error: {e}")
            all_exist = False
    
    return all_exist

def setup_athena_output():
    """Setup Athena output location"""
    print("\n[Step 2] Setting up Athena Output Location...")
    
    output_key = 'athena-results/'
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=output_key, Body=b'')
        print(f"  ✓ s3://{BUCKET_NAME}/{output_key}")
    except Exception as e:
        print(f"  ⚠️  {e}")

def print_next_steps(data_exists):
    """Print instructions"""
    print("\n" + "="*70)
    
    if data_exists:
        print("SETUP COMPLETE")
        print("="*70)
        print("\n✅ Lesson 1 Data Verified:")
        print(f"   • Structured: s3://{BUCKET_NAME}/bronze/orders/")
        print(f"   • Organized: s3://{BUCKET_NAME}/bronze/structured/orders/raw/")
        print(f"   • Athena output: s3://{BUCKET_NAME}/athena-results/")
        
        print("\n📝 Next Steps:")
        print("   Run the exercise:")
        print("      cd starter/")
        print("      python athena_performance_starter.py")
    else:
        print("SETUP INCOMPLETE")
        print("="*70)
        print("\n❌ Missing Lesson 1 Data")
        print("\n   Please complete Lesson 1 exercises first:")
        print("   • Exercise 1: Structured data ingestion")
        print("   • Exercise 3: Data organization with partitions")
        print("\n   Or manually upload data to:")
        print(f"   • s3://{BUCKET_NAME}/bronze/orders/")
        print(f"   • s3://{BUCKET_NAME}/bronze/structured/orders/raw/")

if __name__ == "__main__":
    print("="*70)
    print("LESSON 2 - EXERCISE 1: SETUP VERIFICATION")
    print("="*70)
    
    if not BUCKET_NAME:
        print("\n❌ Error: BUCKET_NAME environment variable not set")
        print("   Run: export BUCKET_NAME='your-bucket-name'")
        exit(1)
    
    try:
        data_exists = verify_lesson1_data()
        setup_athena_output()
        print_next_steps(data_exists)
        
        if not data_exists:
            exit(1)
            
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        exit(1)
