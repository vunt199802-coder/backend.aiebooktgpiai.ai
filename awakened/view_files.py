import boto3
import os
from dotenv import load_dotenv
from tabulate import tabulate
from datetime import datetime
import logging

class FileViewer:
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
        
        self.BUCKET_NAME = "primary-school-ebook-data"

    def list_s3_folders(self):
        """List files in both uploads and indexed folders"""
        print("\n=== S3 Files Overview ===")
        
        for prefix in ['uploads/', 'indexed/']:
            print(f"\n{prefix} Folder Contents:")
            print("-" * 100)
            
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                total_size = 0
                files_count = 0
                
                # Collect all files data first
                files_data = []
                for page in paginator.paginate(Bucket=self.BUCKET_NAME, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        if obj['Key'] == prefix:  # Skip the folder itself
                            continue
                            
                        file_size_mb = obj['Size'] / (1024 * 1024)  # Convert to MB
                        total_size += file_size_mb
                        files_count += 1
                        
                        files_data.append([
                            obj['Key'].split('/')[-1],  # Filename
                            f"{file_size_mb:.2f} MB",
                            obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                        ])
                
                # Display files in table format
                if files_data:
                    print(tabulate(files_data, 
                                 headers=['Filename', 'Size', 'Last Modified'],
                                 tablefmt='grid'))
                    print(f"\nSummary for {prefix}:")
                    print(f"Total Files: {files_count}")
                    print(f"Total Size: {total_size:.2f} MB")
                else:
                    print("No files found")
                    
            except Exception as e:
                print(f"Error listing files in {prefix}: {e}")

    def list_dynamodb_items(self):
        """List all processed files in DynamoDB"""
        print("\n=== DynamoDB Records ===")
        print("-" * 100)
        
        try:
            table = self.dynamodb.Table('ebooks')
            response = table.scan()
            items = response['Items']
            
            # Get remaining items (pagination)
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response['Items'])
            
            if items:
                # Group items by status
                status_groups = {}
                for item in items:
                    status = item.get('status', 'unknown')
                    if status not in status_groups:
                        status_groups[status] = []
                    status_groups[status].append(item)
                
                # Print summary by status
                print("\nSummary by Status:")
                for status, items_list in status_groups.items():
                    print(f"{status}: {len(items_list)} files")
                
                # Print detailed file information
                print("\nDetailed File Information:")
                rows = [[
                    item.get('file_key', 'N/A'),
                    item.get('status', 'N/A'),
                    item.get('upload_time', 'N/A'),
                    item.get('normalized_name', 'N/A')
                ] for item in items]
                
                print(tabulate(rows, 
                             headers=['Original Filename', 'Status', 'Upload Time', 'Normalized Name'],
                             tablefmt='grid'))
                
                print(f"\nTotal Items: {len(items)}")
            else:
                print("No items found")
                
        except Exception as e:
            print(f"Error listing DynamoDB items: {e}")

    def run(self):
        """Run all viewers"""
        print("\nFile Processing Status Report")
        print("=" * 50)
        
        self.list_s3_folders()
        self.list_dynamodb_items()

def main():
    viewer = FileViewer()
    viewer.run()

if __name__ == "__main__":
    main()