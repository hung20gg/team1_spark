import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def chunk_data_by_date(n_days=2):
    """
    Read 3 parquet files, convert created_at to datetime, and chunk data by n-day periods
    starting from 2025-05-05. Data before this date gets labeled as 'initial'.
    
    Args:
        n_days (int): Number of days per chunk (default: 2)
    """
    
    # Define data directory and files
    data_dir = '/home/quanghung20gg/code/spark/fake-system/data'
    parquet_files = ['comments.parquet', 'likes.parquet', 'posts.parquet']
    
    # Define the start date for chunking
    chunk_start_date = datetime(2025, 5, 5)
    
    processed_data = {}
    
    for file_name in parquet_files:
        file_path = os.path.join(data_dir, file_name)
        
        if not os.path.exists(file_path):
            print(f"Warning: {file_name} not found, skipping...")
            continue
            
        print(f"Processing {file_name}...")
        
        # Read the parquet file
        df = pd.read_parquet(file_path)
        if 'label' in df.columns:
            df.drop(columns=['label'], inplace=True)
        
        if 'created_at' not in df.columns:
            print(f"Warning: 'created_at' column not found in {file_name}, skipping...")
            continue
        
        # Convert created_at to datetime if it's not already
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Create chunk labels based on n-day periods
        def assign_chunk(created_at):
            if created_at < chunk_start_date:
                return 'initial'
            else:
                # Calculate days since chunk start date
                days_since_start = (created_at - chunk_start_date).days
                # Group by n-day chunks
                chunk_number = days_since_start // n_days
                return f'chunk_{chunk_number}'
        
        # Apply chunking logic
        df['date_chunk'] = df['created_at'].apply(assign_chunk)
        
        # Sort by created_at for better organization
        df = df.sort_values('created_at')
        
        # Store processed data
        processed_data[file_name] = df
        
        # Print summary
        print(f"  - Total records: {len(df)}")
        print(f"  - Date range: {df['created_at'].min()} to {df['created_at'].max()}")
        print(f"  - Chunk distribution:")
        chunk_counts = df['date_chunk'].value_counts().sort_index()
        for chunk, count in chunk_counts.items():
            print(f"    {chunk}: {count} records")
        print()
    
    # Save processed data into separate folders by chunk
    for file_name, df in processed_data.items():
        # Group by date_chunk and save each chunk to its own folder
        for chunk, chunk_df in df.groupby('date_chunk'):
            if chunk == 'initial':
                folder_name = 'initial'
            else:
                # Extract chunk number and calculate start date
                chunk_num = int(chunk.split('_')[1])
                start_date = chunk_start_date + timedelta(days=chunk_num * n_days)
                folder_name = start_date.strftime('%Y-%m-%d')
            
            # Create folder if it doesn't exist
            folder_path = os.path.join(data_dir, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            
            # Save the chunk data to the folder
            output_file = file_name
            output_path = os.path.join(folder_path, output_file)
            chunk_df = chunk_df.drop(columns=['date_chunk'])  # Drop the helper column before saving
            chunk_df.to_parquet(output_path, index=False)
            
            print(f"Saved {len(chunk_df)} records from {file_name} to {folder_name}/{output_file}")
    
    return processed_data

def get_chunk_date_ranges(n_days=2):
    """
    Helper function to show what date ranges each chunk represents
    
    Args:
        n_days (int): Number of days per chunk (default: 2)
    """
    chunk_start_date = datetime(2025, 5, 5)
    print(f"Chunk date ranges (chunking by {n_days} days):")
    print("initial: All dates before 2025-05-05")
    
    for i in range(10):  # Show first 10 chunks as example
        start_date = chunk_start_date + timedelta(days=i*n_days)
        end_date = start_date + timedelta(days=n_days-1)
        print(f"chunk_{i}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    # Configure chunk size (change this value to chunk by different number of days)
    chunk_days = 7  # Change this to any number of days you want
    
    # Show chunk date ranges
    get_chunk_date_ranges(chunk_days)
    print("\n" + "="*50 + "\n")
    
    # Process the data
    processed_data = chunk_data_by_date(chunk_days)
    
    print(f"\nProcessing complete! Processed {len(processed_data)} files.")
