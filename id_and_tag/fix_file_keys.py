import os
import boto3
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from datetime import datetime

# Initialize console for nice output
console = Console()

def log_info(msg): console.print(f"[blue]ℹ️ {msg}[/blue]")
def log_success(msg): console.print(f"[green]✅ {msg}[/green]")
def log_error(msg): console.print(f"[red]❌ {msg}[/red]")

class FileKeyFixer:
    def __init__(self):
        # Load environment variables
        load_dotenv('.env.id')
        
        # Initialize AWS client
        self.session = boto3.Session(
            aws_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY'),
            region_name=os.getenv('S3_REGION')
        )
        
        self.dynamodb = self.session.resource('dynamodb')
        self.table = self.dynamodb.Table(os.getenv('DYNAMODB_ID_AND_TAG'))
        
        # Statistics
        self.total_items = 0
        self.fixed_items = 0
        self.skipped_items = 0
        self.errors = 0

    def fix_file_keys(self):
        """Check and fix file_keys in DynamoDB table"""
        try:
            log_info("Scanning DynamoDB table...")
            
            # Scan the table
            response = self.table.scan()
            items = response.get('Items', [])
            self.total_items = len(items)
            
            log_info(f"Found {self.total_items} items in table")
            
            # Process each item
            for item in items:
                file_key = item.get('file_key', '')
                
                # Skip if already has compressed/ prefix
                if file_key.startswith('compressed/'):
                    self.skipped_items += 1
                    continue
                
                try:
                    # Create new item with compressed/ prefix
                    new_file_key = f"compressed/{file_key}"
                    item['file_key'] = new_file_key
                    item['timestamp'] = datetime.now().isoformat()
                    
                    # Put new item first
                    self.table.put_item(Item=item)
                    
                    # Delete old item
                    self.table.delete_item(
                        Key={'file_key': file_key}
                    )
                    
                    log_success(f"Updated file_key: {file_key} -> {new_file_key}")
                    self.fixed_items += 1
                    
                except Exception as e:
                    log_error(f"Error fixing {file_key}: {str(e)}")
                    self.errors += 1
            
            # Print summary
            self._print_summary()
            
        except Exception as e:
            log_error(f"Critical error: {str(e)}")
            raise

    def _print_summary(self):
        """Print summary of operations"""
        summary_table = Table(show_header=True, header_style="bold magenta", title="File Key Fix Summary")
        
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Count", justify="right", style="green")
        
        summary_table.add_row("Total Items", str(self.total_items))
        summary_table.add_row("Fixed Items", str(self.fixed_items))
        summary_table.add_row("Skipped Items", str(self.skipped_items))
        summary_table.add_row("Errors", f"[red]{self.errors}[/red]")
        
        console.print("\n")
        console.print(summary_table)

def main():
    try:
        fixer = FileKeyFixer()
        fixer.fix_file_keys()
    except Exception as e:
        log_error(f"Script failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
