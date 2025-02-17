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

def clean_state_labels(df):
    # Define a mapping of full state names to their abbreviations
    state_mapping = {
        'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
        'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
        'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
        'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
        'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
        'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
        'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
        'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
        'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
        'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
    }

    # Apply cleaning only to players born in the USA
    usa_players = df['BirthCountry'] == 'Usa'

    # Strip whitespace and convert to uppercase
    df.loc[usa_players, 'BirthState'] = df.loc[usa_players, 'BirthState'].str.strip().str.upper()

    # Replace full state names with abbreviations
    df.loc[usa_players, 'BirthState'] = df.loc[usa_players, 'BirthState'].replace(state_mapping)
    
    additional_mapping = {
        'NORTH CAROLINA': 'NC',
        'FLORIDA': 'FL',
        'PENNSYLVANIA': 'PA',
        'UTAH': 'UT',
        'ARKANSAS': 'AR',
        'NEW MEXICO': 'NM',
        'CALIFORNIA': 'CA',
        'SOUTH CAROLINA': 'SC',
        'OKLAHOMA': 'OK',
        'NEW YORK': 'NY',
        'KENTUCKY': 'KY',
        'CAL': 'CA',
        'USA': 'US'
    }

    df.loc[usa_players, 'BirthState'] = df.loc[usa_players, 'BirthState'].replace(additional_mapping)

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
        # Clean state labels
    df = clean_state_labels(df)


    return df

def transform_data(file_paths):
    processed_dir = os.getenv('PROCESSED_DATA_PATH')
    os.makedirs(processed_dir, exist_ok=True)
    
    transformed_data_list = []

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
        
                # Append the cleaned data to the list
        transformed_data_list.append(cleaned_data)

    return transformed_data_list
#Example usage
if __name__ == "__main__":
    # List of file paths to process
    file_paths = [
        os.getenv('CLEANED_DATA_PATH'),
        os.getenv('RAW_PLAYERS_DATA_PATH')
    ]
    
    transform_data(file_paths)