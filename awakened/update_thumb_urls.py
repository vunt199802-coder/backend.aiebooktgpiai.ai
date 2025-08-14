import boto3
import os
from dotenv import load_dotenv

load_dotenv(".env")

# Initialize DynamoDB resource
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("DYNAMODB_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

EBOOK_TABLE = "ebook-store"

def find_epub_files():
    table = dynamodb.Table(EBOOK_TABLE)
    
    # Scan the table to get all items
    response = table.scan()
    items = response.get('Items', [])
    
    affected_files = []
    for item in items:
        file_key = item.get('file_key', '')
        thumb_url = item.get('thumb_url', '')
        
        # Check if file is epub and thumb_url ends with .epub
        if file_key.lower().endswith('.epub') and thumb_url.lower().endswith('.epub'):
            # Create new thumb_url by replacing .epub with .pdf
            base_url = thumb_url.rsplit('.', 1)[0]
            new_thumb_url = base_url + '.pdf'
            
            affected_files.append({
                'file_key': file_key,
                'old_thumb_url': thumb_url,
                'new_thumb_url': new_thumb_url
            })
    
    return affected_files

def update_thumb_urls(affected_files):
    table = dynamodb.Table(EBOOK_TABLE)
    update_count = 0
    
    for file in affected_files:
        try:
            table.update_item(
                Key={'file_key': file['file_key']},
                UpdateExpression='SET thumb_url = :new_thumb_url',
                ExpressionAttributeValues={
                    ':new_thumb_url': file['new_thumb_url']
                }
            )
            update_count += 1
            print(f"Updated thumb_url for {file['file_key']}")
            print(f"Old URL: {file['old_thumb_url']}")
            print(f"New URL: {file['new_thumb_url']}")
            print("-" * 50)
        except Exception as e:
            print(f"Error updating {file['file_key']}: {str(e)}")
    
    print(f"\nTotal items updated: {update_count}")

if __name__ == "__main__":
    # First, find all epub files with .epub thumb_urls
    affected_files = find_epub_files()
    
    if not affected_files:
        print("No .epub files found with .epub thumb_urls.")
        exit()
    
    print(f"\nFound {len(affected_files)} files that will be updated:")
    print("-" * 50)
    for file in affected_files:
        print(f"File Key: {file['file_key']}")
        print(f"Current thumb_url: {file['old_thumb_url']}")
        print(f"Will be changed to: {file['new_thumb_url']}")
        print("-" * 50)
    
    # Ask for confirmation
    confirmation = input("\nDo you want to proceed with these updates? (yes/no): ")
    
    if confirmation.lower() == 'yes':
        print("\nProceeding with updates...")
        update_thumb_urls(affected_files)
    else:
        print("\nUpdate cancelled.")
