import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
import docx
import os
from datetime import datetime
import re

st.set_page_config(page_title="IC Number Bulk Upload", layout="wide")

def get_school_name(filename):
    """Extract school name from filename without extension."""
    return os.path.splitext(filename)[0]

def clean_ic_number(ic_number):
    """Clean IC number and ensure proper formatting with leading zeros."""
    # Convert to string and handle scientific notation
    ic_str = str(ic_number)
    
    # Remove any decimal points and zeros after them
    ic_str = re.sub(r'\.0*$', '', ic_str)
    
    # Remove any other decimal points and non-numeric characters
    ic_str = re.sub(r'[^0-9]', '', ic_str)
    
    # Check if the number needs padding
    if len(ic_str) < 12 and len(ic_str) >= 10:  # Potentially missing leading zeros
        # Pad with zeros until reaching 12 digits
        ic_str = ic_str.zfill(12)
    
    return ic_str

def validate_ic_number(ic_number):
    """Validate IC number after cleaning."""
    cleaned_ic = clean_ic_number(ic_number)
    
    # Must be between 12 and 14 digits
    if not (12 <= len(cleaned_ic) <= 14):
        return False
        
    # Additional validation for Malaysian IC format
    # First 6 digits represent YYMMDD
    if len(cleaned_ic) == 12:
        yy = int(cleaned_ic[0:2])
        mm = int(cleaned_ic[2:4])
        dd = int(cleaned_ic[4:6])
        
        # Basic date validation
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            return False
    
    return True

def init_dynamodb():
    """Initialize DynamoDB client."""
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"],
        region_name=st.secrets["region_name"]
    )
    return dynamodb

def process_excel(file, school_name):
    """Process Excel files."""
    df = pd.read_excel(file)
    
    # Check for required columns
    required_columns = ['NAME', 'IC NUMBER']
    if not all(col in df.columns for col in required_columns):
        st.error(f"Required columns {required_columns} not found in the Excel file.")
        return None
    
    # Clean the data
    df['IC NUMBER'] = df['IC NUMBER'].apply(clean_ic_number)
    df['NAME'] = df['NAME'].astype(str).str.strip()
    df['SCHOOL'] = school_name
    
    # Add original IC number column for comparison
    df['ORIGINAL IC'] = df['IC NUMBER'].copy()
    
    # Validate IC numbers
    df['IC_VALID'] = df['IC NUMBER'].apply(validate_ic_number)
    
    # Filter valid entries and create result
    valid_entries = df[df['IC_VALID']].copy()
    invalid_entries = df[~df['IC_VALID']].copy()
    
    return valid_entries[['NAME', 'IC NUMBER', 'ORIGINAL IC', 'SCHOOL']], invalid_entries[['NAME', 'IC NUMBER', 'ORIGINAL IC', 'SCHOOL']]

def process_word(file, school_name):
    """Process Word documents."""
    # Save the uploaded file temporarily
    with open("temp.docx", "wb") as f:
        f.write(file.getvalue())
    
    # Read the document
    doc = docx.Document("temp.docx")
    
    # Create empty lists for valid and invalid entries
    valid_entries = []
    invalid_entries = []
    
    # Process tables in the document
    for table in doc.tables:
        # Find header row to identify columns
        header_row = [cell.text.strip().upper() for cell in table.rows[0].cells]
        
        try:
            name_idx = header_row.index('NAME')
            ic_idx = header_row.index('IC NUMBER')
        except ValueError:
            continue  # Skip tables without required columns
        
        # Process each row after header
        for row in table.rows[1:]:
            try:
                name = row.cells[name_idx].text.strip()
                original_ic = row.cells[ic_idx].text.strip()
                ic_number = clean_ic_number(original_ic)
                
                # Validate IC number
                is_valid = validate_ic_number(ic_number)
                
                entry = {
                    'NAME': name,
                    'IC NUMBER': ic_number,
                    'ORIGINAL IC': original_ic,
                    'SCHOOL': school_name
                }
                
                if is_valid:
                    valid_entries.append(entry)
                else:
                    invalid_entries.append(entry)
                    
            except IndexError:
                continue  # Skip malformed rows
    
    # Clean up
    os.remove("temp.docx")
    
    # Convert to DataFrames
    valid_df = pd.DataFrame(valid_entries)
    invalid_df = pd.DataFrame(invalid_entries)
    
    return valid_df, invalid_df

def upload_to_dynamodb(dynamodb, data_df):
    """Upload data to DynamoDB."""
    table = dynamodb.Table('IC_Numbers')
    success_count = 0
    error_count = 0
    skip_count = 0
    errors = []
    total_records = len(data_df)
    
    # Remove duplicates within the dataframe itself
    original_count = len(data_df)
    data_df = data_df.drop_duplicates(subset=['IC NUMBER'], keep='first')
    internal_duplicates = original_count - len(data_df)
    if internal_duplicates > 0:
        st.warning(f"Found {internal_duplicates} duplicate IC numbers within the uploaded files. Only the first occurrence will be processed.")
    
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
    
    try:
        dynamodb = init_dynamodb()
    except Exception as e:
        st.error(f"Failed to initialize DynamoDB: {str(e)}")
        return
    
    st.info("""Upload Excel or Word files containing NAME and IC NUMBER columns. 
    The app will automatically:
    - Clean IC numbers by removing decimal points
    - Add leading zeros if needed
    - Validate the IC number format
    - Track entries by school name""")
    
    uploaded_files = st.file_uploader(
        "Upload Excel (.xlsx) or Word (.docx) files",
        type=["xlsx", "xls", "docx"],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        all_valid_entries = pd.DataFrame()
        all_invalid_entries = pd.DataFrame()
        
        for file in uploaded_files:
            school_name = get_school_name(file.name)
            st.write(f"Processing {school_name}...")
            
            try:
                if file.name.endswith(('.xlsx', '.xls')):
                    result = process_excel(file, school_name)
                else:  # .docx
                    result = process_word(file, school_name)
                    
                if result is not None:
                    valid_entries, invalid_entries = result
                    
                    if len(valid_entries) > 0:
                        all_valid_entries = pd.concat([all_valid_entries, valid_entries])
                    if len(invalid_entries) > 0:
                        all_invalid_entries = pd.concat([all_invalid_entries, invalid_entries])
                    
                    # Show modifications made
                    modifications = valid_entries[valid_entries['IC NUMBER'] != valid_entries['ORIGINAL IC']]
                    if len(modifications) > 0:
                        st.info(f"Modified {len(modifications)} IC numbers in {school_name} by adding leading zeros or cleaning format")
                    
                    st.success(f"Found {len(valid_entries)} valid entries in {school_name}")
                    if len(invalid_entries) > 0:
                        st.warning(f"Found {len(invalid_entries)} invalid entries in {school_name}")
                    
            except Exception as e:
                st.error(f"Error processing {file.name}: {str(e)}")
        
        # Display results if we have any valid entries
        if not all_valid_entries.empty:
            st.write("### Valid Entries Preview")
            st.dataframe(all_valid_entries[['NAME', 'IC NUMBER', 'SCHOOL']])
            
            with st.expander("Show Original vs Cleaned IC Numbers"):
                st.dataframe(all_valid_entries)
            
            # Download button for valid entries
            csv_valid = all_valid_entries.to_csv(index=False)
            st.download_button(
                label="Download Valid Entries",
                data=csv_valid,
                file_name="valid_entries.csv",
                mime="text/csv"
            )
            
            if not all_invalid_entries.empty:
                st.write("### Invalid Entries")
                # Group invalid entries by school
                for school in all_invalid_entries['SCHOOL'].unique():
                    with st.expander(f"Invalid Entries for {school}"):
                        school_entries = all_invalid_entries[all_invalid_entries['SCHOOL'] == school]
                        st.dataframe(school_entries[['NAME', 'IC NUMBER', 'ORIGINAL IC']])
                
                # Download button for invalid entries
                csv_invalid = all_invalid_entries.to_csv(index=False)
                st.download_button(
                    label="Download Invalid Entries",
                    data=csv_invalid,
                    file_name="invalid_entries.csv",
                    mime="text/csv"
                )
                
                # Summary of invalid entries by school
                with st.expander("Invalid Entries Summary"):
                    summary = all_invalid_entries.groupby('SCHOOL').size().reset_index()
                    summary.columns = ['School', 'Number of Invalid Entries']
                    st.dataframe(summary)
            
            # Upload button
            if st.button("Upload to DynamoDB"):
                success_count, error_count, skip_count, errors = upload_to_dynamodb(dynamodb, all_valid_entries)
                
                st.success(f"Successfully uploaded {success_count} entries")
                if skip_count > 0:
                    st.warning(f"Skipped {skip_count} entries that already exist in the database")
                
                if error_count > 0:
                    with st.expander("Show Errors"):
                        for error in errors:
                            st.error(error)

if __name__ == "__main__":
    main()