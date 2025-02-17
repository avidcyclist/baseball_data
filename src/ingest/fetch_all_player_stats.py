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

def fetch_player_season_stats(api_key, season):
    url = f"https://api.sportsdata.io/v3/mlb/stats/json/PlayerSeasonStats/{season}?key={api_key}"
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    return ingest_data_from_api(url, headers)

def store_json(data, json_file_path):
    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def store_players_in_csv(players, csv_file_path):
    df = pd.DataFrame(players)
    df.to_csv(csv_file_path, index=False)

def ingest_player_season_stats(season):
    api_key = os.getenv('SPORTS_DATA_API_KEY')
    players = fetch_player_season_stats(api_key, season)
    
    raw_data_path = os.getenv('RAW_STATS_DATA_PATH')
    json_file_path = os.path.join(raw_data_path, 'player_season_stats.json')
    csv_file_path = os.path.join(raw_data_path, 'player_season_stats.csv')
    
    store_json(players, json_file_path)
    store_players_in_csv(players, csv_file_path)

    # Print the first few rows of the DataFrame to inspect the data
    df = pd.DataFrame(players)
    print("Player Season Stats Data:")
    print(df.head())

# Example usage
if __name__ == "__main__":
    season = '2024'  # Specify the season you want to fetch stats for
    ingest_player_season_stats(season)