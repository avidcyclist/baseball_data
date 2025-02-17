import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def clean_country_names(df):
    # Define a mapping of incorrect country names to correct ones
    country_mapping = {
        'domincan republic': 'Dominican Republic',
        'dominican rublic': 'Dominican Republic',
        'dominincan republic': 'Dominican Republic',
        'domininican republic': 'Dominican Republic',
        'domican republic': 'Dominican Republic',
        'dominicon republic': 'Dominican Republic',
        'dominican repuplic': 'Dominican Republic',
        'domnican republic': 'Dominican Republic',
        'venezula': 'Venezuela',
        'venezuala': 'Venezuela',
        'venezeula': 'Venezuela',
        'venequela': 'Venezuela',
        'columbia': 'Colombia',
        'puerto rico': 'Puerto Rico',
        'usa`': 'USA',
        'az': 'USA',
        'ca': 'USA'
    }

    # Strip whitespace and convert to lowercase
    df['BirthCountry'] = df['BirthCountry'].str.strip().str.lower()

    # Replace incorrect country names with correct ones
    df['BirthCountry'] = df['BirthCountry'].replace(country_mapping)

    # Convert back to title case for consistency
    df['BirthCountry'] = df['BirthCountry'].str.title()

    return df

def clean_data(df):
    # Handle missing values
    df.fillna(method='ffill', inplace=True)  # Forward fill for missing values

    # Drop rows where critical columns have missing values
    critical_columns = ['PlayerID', 'FirstName', 'LastName', 'BirthDate']
    df.dropna(subset=critical_columns, inplace=True)

    # Convert data types
    df['BirthDate'] = pd.to_datetime(df['BirthDate'], errors='coerce')  # Convert BirthDate column to datetime
    df['PlayerID'] = df['PlayerID'].astype(str)  # Ensure PlayerID is a string

    # Clean country names
    df = clean_country_names(df)

    return df

def transform_data(file_paths):
    processed_dir = os.getenv('PROCESSED_DATA_PATH')
    os.makedirs(processed_dir, exist_ok=True)

    for file_path in file_paths:
        if file_path is None:
            print("Error: One of the file paths is not set in the environment variables.")
            continue

        # Load the CSV into a DataFrame
        raw_data = pd.read_csv(file_path)

        # Clean the data
        cleaned_data = clean_data(raw_data)

        # Extract the file name from the file path
        file_name = os.path.basename(file_path).replace('.csv', '')

        # Save the cleaned data to a new CSV file
        cleaned_data.to_csv(f'{processed_dir}/cleaned_{file_name}.csv', index=False)

        print(f"Processed and saved: {file_name}")

# Example usage
if __name__ == "__main__":
    # List of file paths to process
    file_paths = [
        os.getenv('CLEANED_DATA_PATH'),
        os.getenv('RAW_PLAYERS_DATA_PATH')
    ]
    
    transform_data(file_paths)