import pandas as pd
import requests
from dotenv import load_dotenv
import os
import time

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

def fetch_team_roster(team, api_key):
    url = f"https://api.sportsdata.io/v3/mlb/scores/json/PlayersBasic/{team}?key={api_key}"
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    return ingest_data_from_api(url, headers)

def store_players_in_csv(players, csv_file_path):
    df = pd.DataFrame(players)
    df.to_csv(csv_file_path, index=False)

def ingest_marlins_roster():
    api_key = os.getenv('SPORTS_DATA_API_KEY')
    team = 'MIA'
    players = fetch_team_roster(team, api_key)
    csv_file_path = os.getenv('RAW_DATA_PATH')
    store_players_in_csv(players, csv_file_path)

# Example usage
if __name__ == "__main__":
    ingest_marlins_roster()