import pandas as pd
import requests
from dotenv import load_dotenv
import os
import time
import json

# Load environment variables from .env file
load_dotenv()

def ingest_data_from_api(api_url, headers, retries=3, delay=5):
    for attempt in range(retries):
        print(f"Fetching data from API: {api_url} (Attempt {attempt + 1})")
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # Print out the first item to inspect for timestamps
            if data:
                print("Sample data:", data[0])
            return data
        else:
            print(f"Failed to fetch data from API. Status code: {response.status_code}, Response: {response.text}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise Exception("Failed to fetch data from API after multiple attempts.")

def fetch_all_active_players(api_key):
    url = f"https://api.sportsdata.io/v3/mlb/scores/json/Players?key={api_key}"
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    return ingest_data_from_api(url, headers)


def convert_datetime_format(players):
    for player in players:
        if 'BirthDate' in player and player['BirthDate']:
            player['BirthDate'] = pd.to_datetime(player['BirthDate'], format='%Y-%m-%dT%H:%M:%S', errors='coerce').strftime('%Y-%m-%d')
        if 'ProDebut' in player and player['ProDebut']:
            player['ProDebut'] = pd.to_datetime(player['ProDebut'], format='%Y-%m-%dT%H:%M:%S', errors='coerce').strftime('%Y-%m-%d')
    return players

def store_json(data, json_file_path):
    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def store_players_in_csv(players, csv_file_path):
    df = pd.DataFrame(players)
    df.to_csv(csv_file_path, index=False)

def ingest_all_active_players():
    api_key = os.getenv('SPORTS_DATA_API_KEY')
    players = fetch_all_active_players(api_key)
        # Convert datetime format
    players = convert_datetime_format(players)
    json_file_path = os.getenv('ALL_PLAYERS_JSON_PATH')
    store_json(players, json_file_path)

    # Convert JSON to DataFrame and save as CSV
    df = pd.DataFrame(players)
    csv_file_path = os.getenv('ALL_PLAYERS_CSV_PATH')
    df.to_csv(csv_file_path, index=False)

    # Print the first few rows of the DataFrame to inspect the data
    print("All Active Players Data:")
    print(df.head())

# Example usage
if __name__ == "__main__":
    ingest_all_active_players()