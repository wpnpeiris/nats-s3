#!/usr/bin/env python3
"""
NATS-S3 Python Example using Boto3

This example demonstrates how to use the NATS-S3 gateway with Python's boto3 library.

Prerequisites:
    pip install boto3

Usage:
    python nats_s3_example.py
"""

import boto3
from botocore.exceptions import ClientError
import sys

# NATS-S3 Configuration
ENDPOINT_URL = 'http://localhost:5222'
AWS_ACCESS_KEY_ID = 'demo-access-key'
AWS_SECRET_ACCESS_KEY = 'demo-secret-key'
AWS_REGION = 'us-east-1'

def create_s3_client():
    """Create and return an S3 client configured for NATS-S3"""
    return boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        # Disable SSL verification for local testing
        use_ssl=False,
        verify=False
    )

def main():
    """Run example S3 operations"""
    s3 = create_s3_client()
    bucket_name = 'python-example-bucket'

    print("=" * 60)
    print("NATS-S3 Python Example with Boto3")
    print("=" * 60)

    try:
        # 1. Create bucket
        print(f"\n1. Creating bucket '{bucket_name}'...")
        s3.create_bucket(Bucket=bucket_name)
        print("   ✓ Bucket created successfully")

        # 2. List buckets
        print("\n2. Listing all buckets...")
        response = s3.list_buckets()
        for bucket in response['Buckets']:
            print(f"   - {bucket['Name']}")

        # 3. Create a test file and upload it
        print(f"\n3. Uploading object to '{bucket_name}'...")
        test_content = "Hello from NATS-S3!\nThis is a test file created by Python."
        test_key = 'test-file.txt'

        s3.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content.encode('utf-8'),
            ContentType='text/plain',
            Metadata={'created-by': 'python-example', 'version': '1.0'}
        )
        print(f"   ✓ Uploaded '{test_key}'")

        # 4. Upload another file with different content
        print(f"\n4. Uploading additional objects...")
        s3.put_object(
            Bucket=bucket_name,
            Key='data/numbers.txt',
            Body='1\n2\n3\n4\n5\n'.encode('utf-8')
        )
        s3.put_object(
            Bucket=bucket_name,
            Key='data/letters.txt',
            Body='a\nb\nc\nd\ne\n'.encode('utf-8')
        )
        print("   ✓ Uploaded 'data/numbers.txt'")
        print("   ✓ Uploaded 'data/letters.txt'")

        # 5. List objects in bucket
        print(f"\n5. Listing objects in '{bucket_name}'...")
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"   - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("   (bucket is empty)")

        # 6. Get object metadata
        print(f"\n6. Getting metadata for '{test_key}'...")
        response = s3.head_object(Bucket=bucket_name, Key=test_key)
        print(f"   - Content-Type: {response['ContentType']}")
        print(f"   - Content-Length: {response['ContentLength']} bytes")
        print(f"   - Last-Modified: {response['LastModified']}")
        if 'Metadata' in response:
            print(f"   - Custom Metadata: {response['Metadata']}")

        # 7. Download and read object
        print(f"\n7. Downloading '{test_key}'...")
        response = s3.get_object(Bucket=bucket_name, Key=test_key)
        content = response['Body'].read().decode('utf-8')
        print(f"   Content:\n   {content.replace(chr(10), chr(10) + '   ')}")

        # 8. List objects with prefix (hierarchical listing)
        print(f"\n8. Listing objects with prefix 'data/'...")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='data/')
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"   - {obj['Key']}")

        # 9. Copy object
        print(f"\n9. Copying object...")
        copy_source = {'Bucket': bucket_name, 'Key': test_key}
        s3.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,
            Key='test-file-copy.txt'
        )
        print("   ✓ Copied 'test-file.txt' to 'test-file-copy.txt'")

        # 10. Generate presigned URL
        print(f"\n10. Generating presigned URL for '{test_key}'...")
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': test_key},
            ExpiresIn=3600  # 1 hour
        )
        print(f"   URL: {url[:80]}...")

        # 11. Delete specific objects
        print(f"\n11. Deleting objects...")
        s3.delete_object(Bucket=bucket_name, Key='test-file-copy.txt')
        print("   ✓ Deleted 'test-file-copy.txt'")

        # 12. Batch delete
        print(f"\n12. Batch deleting remaining objects...")
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            print(f"   ✓ Deleted {len(objects_to_delete)} objects")

        # 13. Delete bucket
        print(f"\n13. Deleting bucket '{bucket_name}'...")
        s3.delete_bucket(Bucket=bucket_name)
        print("   ✓ Bucket deleted successfully")

        print("\n" + "=" * 60)
        print("✓ All operations completed successfully!")
        print("=" * 60 + "\n")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"\n✗ Error ({error_code}): {error_message}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
