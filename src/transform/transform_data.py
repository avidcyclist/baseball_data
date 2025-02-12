import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def clean_data(df):
    # Handle missing values
    df.fillna(method='ffill', inplace=True)  # Forward fill for missing values

    # Drop rows where critical columns have missing values
    critical_columns = ['PlayerID', 'FirstName', 'LastName', 'BirthDate']
    df.dropna(subset=critical_columns, inplace=True)

    # Convert data types
    df['BirthDate'] = pd.to_datetime(df['BirthDate'], errors='coerce')  # Convert BirthDate column to datetime
    df['PlayerID'] = df['PlayerID'].astype(str)  # Ensure PlayerID is a string

    return df

def transform_data(file_path):
    # Load the CSV into a DataFrame
    raw_data = pd.read_csv(file_path)

    # Clean the data
    cleaned_data = clean_data(raw_data)

    # Get the processed data directory from environment variables
    processed_dir = os.getenv('PROCESSED_DATA_PATH')
    os.makedirs(processed_dir, exist_ok=True)

    # Save the cleaned data to a new CSV file
    cleaned_data.to_csv(f'{processed_dir}/cleaned_marlins_roster.csv', index=False)

    return cleaned_data

# Example usage
if __name__ == "__main__":
    file_path = os.getenv('RAW_DATA_PATH')
    cleaned_data = transform_data(file_path)
    print(cleaned_data.head())