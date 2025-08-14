import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
import docx
import os
from datetime import datetime

st.set_page_config(page_title="IC Number Bulk Upload", layout="wide")

def clean_ic_number(ic_number):
    """Clean IC number by converting to string and removing whitespace."""
    # Convert to string and handle any potential NaN values
    ic_str = str(ic_number)
    if ic_str.lower() == 'nan':
        return ''
    
    # Remove whitespace and convert to uppercase
    return ic_str.strip().upper()

def process_excel(file):
    """Process Excel files with NAME and IC NUMBER columns."""
    try:
        df = pd.read_excel(file)
        
        # Check if the dataframe is empty
        if df.empty:
            st.error("The uploaded Excel file is empty.")
            return None
        
        # Check for required columns
        required_columns = ['NAME', 'IC NUMBER']
        if not all(col in df.columns for col in required_columns):
            st.error(f"Required columns not found. Please ensure your Excel file has these columns: {required_columns}")
            return None
        
        # Remove any unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Clean the data
        df['NAME'] = df['NAME'].astype(str).str.strip().str.upper()
        df['IC NUMBER'] = df['IC NUMBER'].apply(clean_ic_number)
        df['SCHOOL'] = ""  # Empty school field
        
        # Remove rows where NAME or IC NUMBER is empty
        df = df.dropna(subset=['NAME', 'IC NUMBER'])
        df = df[df['IC NUMBER'] != '']
        
        if len(df) == 0:
            st.warning("No valid entries found in the Excel file.")
            return None
        
        return df[['NAME', 'IC NUMBER', 'SCHOOL']]
        
    except Exception as e:
        st.error(f"Error processing Excel file: {str(e)}")
        return None

def process_word(file):
    """Process Word documents."""
    try:
        # Save the uploaded file temporarily
        with open("temp.docx", "wb") as f:
            f.write(file.getvalue())
        
        # Read the document
        doc = docx.Document("temp.docx")
        
        # Process each paragraph
        entries = []
        for para in doc.paragraphs:
            text = para.text.strip()
            if text:  # Skip empty paragraphs
                # Split the text into name and IC number
                parts = text.split()
                if len(parts) >= 2:
                    # Last part is IC number, rest is name
                    ic_number = parts[-1].strip()
                    name = ' '.join(parts[:-1]).strip()
                    
                    if name and ic_number:  # Only add if both fields are present
                        entries.append({
                            'NAME': name.upper(),
                            'IC NUMBER': clean_ic_number(ic_number),
                            'SCHOOL': ""
                        })
        
        if not entries:
            st.warning("No valid entries found in the Word document.")
            return None
            
        return pd.DataFrame(entries)
        
    except Exception as e:
        st.error(f"Error processing Word file: {str(e)}")
        return None
        
    finally:
        # Clean up temporary file
        if os.path.exists("temp.docx"):
            os.remove("temp.docx")

def init_dynamodb():
    """Initialize DynamoDB client."""
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"],
        region_name=st.secrets["region_name"]
    )
    return dynamodb

def upload_to_dynamodb(dynamodb, data_df):
    """Upload data to DynamoDB."""
    table = dynamodb.Table('IC_Numbers')
    success_count = 0
    error_count = 0
    skip_count = 0
    errors = []
    total_records = len(data_df)
    
    with st.spinner('Uploading to DynamoDB...'):
        # Create two columns for progress and time
        col1, col2 = st.columns(2)
        
        # Initialize progress bar with count display
        progress_text = f"Uploading IC numbers (0/{len(data_df)})"
        progress_bar = col1.progress(0, text=progress_text)
        # Initialize time status
        time_status = col2.empty()
        
        start_time = datetime.now()
        processed_count = 0
        
        for i, row in data_df.iterrows():
            try:
                # Check if IC number already exists
                response = table.query(
                    KeyConditionExpression=Key('icNumber').eq(str(row['IC NUMBER']))
                )
                
                if response['Items']:
                    skip_count += 1
                else:
                    current_time = datetime.now().isoformat()
                    
                    item = {
                        'icNumber': str(row['IC NUMBER']),
                        'createdAt': current_time,
                        'registrationStatus': 'APPROVED',
                        'name': row['NAME'],
                        'school': row['SCHOOL']
                    }
                    
                    table.put_item(Item=item)
                    success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append(f"Error uploading {row['NAME']} (IC: {row['IC NUMBER']}) from {row['SCHOOL']}: {str(e)}")
            
            # Update progress with count display
            processed_count += 1
            progress = processed_count / len(data_df)
            progress_text = f"Uploading IC numbers ({processed_count}/{len(data_df)})"
            progress_bar.progress(progress, text=progress_text)
            
            # Calculate and display time remaining
            elapsed_time = (datetime.now() - start_time).total_seconds()
            if processed_count > 0:  # Avoid division by zero
                time_per_record = elapsed_time / processed_count
                remaining_records = len(data_df) - processed_count
                estimated_time_remaining = remaining_records * time_per_record
                
                time_status.text(f"Time remaining: {estimated_time_remaining:.1f} seconds")
    
    return success_count, error_count, skip_count, errors

def main():
    st.title("IC Number Bulk Upload")
    
    st.markdown("""
    Upload Excel (.xlsx) or Word (.docx) files containing NAME and IC NUMBER columns.
    
    The app will:
    - Convert names to uppercase
    - Remove any extra whitespace
    - Convert IC numbers to uppercase""")
    
    uploaded_files = st.file_uploader(
        "Upload Excel (.xlsx) or Word (.docx) files",
        type=["xlsx", "docx"],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        dynamodb = init_dynamodb()
        all_entries = pd.DataFrame()
        
        for file in uploaded_files:
            st.write(f"Processing {file.name}...")
            
            try:
                if file.name.endswith(('.xlsx', '.xls')):
                    result = process_excel(file)
                else:  # .docx
                    result = process_word(file)
                    
                if result is not None:
                    all_entries = pd.concat([all_entries, result], ignore_index=True)
                    st.success(f"Found {len(result)} entries")
                    
            except Exception as e:
                st.error(f"Error processing {file.name}: {str(e)}")
        
        # Display results if we have any entries
        if not all_entries.empty:
            st.write("### Entries Preview")
            st.dataframe(all_entries[['NAME', 'IC NUMBER']])
            
            # Upload button
            if st.button("Upload to DynamoDB"):
                success_count, error_count, skip_count, errors = upload_to_dynamodb(dynamodb, all_entries)
                
                st.success(f"Successfully uploaded {success_count} entries")
                if skip_count > 0:
                    st.warning(f"Skipped {skip_count} entries that already exist in the database")
                
                if error_count > 0:
                    with st.expander("Show Errors"):
                        for error in errors:
                            st.error(error)

if __name__ == "__main__":
    main()