from collections import Counter
import pandas as pd
from datetime import datetime

def analyze_transfer_errors():
    # Read the error log file
    with open('transfer_log_errors.txt', 'r') as f:
        lines = f.readlines()
    
    # Extract addresses
    addresses = []
    for line in lines:
        parts = line.strip().split('-')
        if len(parts) >= 2:
            address = parts[1].strip()
            addresses.append(address)
    
    # Create a DataFrame
    df = pd.DataFrame({'address': addresses})
    
    # Count errors by address
    error_counts = df['address'].value_counts().reset_index()
    error_counts.columns = ['address', 'error_count']
    
    # Calculate percentages
    total_errors = len(df)
    error_counts['percentage'] = (error_counts['error_count'] / total_errors * 100).round(2)
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f'error_analysis_{timestamp}.csv'
    error_counts.to_csv(output_file, index=False)
    
    print(f"\nAnalysis has been saved to: {output_file}")
    print("\nFirst few rows of the saved data:")
    print(error_counts.head().to_string())

if __name__ == "__main__":
    analyze_transfer_errors()
