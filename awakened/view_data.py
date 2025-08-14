import boto3
import os
from dotenv import load_dotenv
from pinecone import Pinecone
from tabulate import tabulate
from datetime import datetime

class DataViewer:
    def __init__(self):
        load_dotenv(".env")
        
        # Initialize AWS clients
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("S3_REGION")
        )
        
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.getenv("DYNAMODB_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        # Initialize Pinecone with error handling
        try:
            self.pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
            self.index = self.pc.Index(os.getenv("PINECONE_INDEX"))
        except Exception as e:
            print(f"Warning: Could not connect to Pinecone index: {e}")
            self.pc = None
            self.index = None

    def list_s3_files(self):
        """List files in both original and indexed S3 buckets"""
        print("\n=== S3 Files ===")
        
        buckets = {
            "Original": ("primary-school-ebook-data", "uploads/"),
            "Indexed": ("primary-school-ebook-data", "indexed/")
        }
        
        for bucket_type, (bucket_name, prefix) in buckets.items():
            print(f"\n{bucket_type} Bucket ({bucket_name}/{prefix}):")
            print("-" * 100)
            
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                rows = []
                
                for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        # Skip the directory itself
                        if obj['Key'] == prefix:
                            continue
                            
                        rows.append([
                            obj['Key'].split('/')[-1],  # Filename
                            f"{obj['Size']/1024/1024:.2f} MB",  # Size in MB
                            obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')  # Last modified
                        ])
                
                if rows:
                    print(tabulate(rows, headers=['Filename', 'Size', 'Last Modified'], 
                                 tablefmt='grid'))
                    print(f"Total files: {len(rows)}")
                else:
                    print("No files found")
                    
            except Exception as e:
                print(f"Error listing files in {bucket_type} bucket: {e}")

    def list_dynamodb_items(self):
        """List all items in DynamoDB table"""
        print("\n=== DynamoDB Items ===")
        print("-" * 100)
        
        try:
            table = self.dynamodb.Table('ebooks')
            response = table.scan()
            items = response['Items']
            
            # Continue scanning if we have more items (pagination)
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response['Items'])
            
            if items:
                # Prepare rows for tabulate
                rows = [[
                    item.get('file_key', 'N/A'),
                    item.get('status', 'N/A'),
                    item.get('upload_time', 'N/A'),
                ] for item in items]
                
                print(tabulate(rows, headers=['File Key', 'Status', 'Upload Time'], 
                             tablefmt='grid'))
                print(f"Total items: {len(items)}")
            else:
                print("No items found")
                
        except Exception as e:
            print(f"Error listing DynamoDB items: {e}")

    def list_pinecone_stats(self):
        """Show statistics about Pinecone vectors"""
        print("\n=== Pinecone Index Stats ===")
        print("-" * 100)
        
        if not self.index:
            print("No Pinecone index available")
            return
        
        try:
            # Get index statistics
            stats = self.index.describe_index_stats()
            
            # Format the statistics
            print(f"Total vectors: {stats['total_vector_count']:,}")
            print(f"Dimension: {stats['dimension']}")
            
            # Show namespaces if any
            if 'namespaces' in stats:
                print("\nNamespaces:")
                for ns, details in stats['namespaces'].items():
                    print(f"- {ns}: {details['vector_count']:,} vectors")
                    
        except Exception as e:
            print(f"Error getting Pinecone stats: {e}")

    def run(self):
        """Run all viewers"""
        print("\nData Viewer Report")
        print("=" * 50)
        
        self.list_s3_files()
        self.list_dynamodb_items()
        self.list_pinecone_stats()

if __name__ == "__main__":
    viewer = DataViewer()
    viewer.run()