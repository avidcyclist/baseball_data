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

def convert_datetime_format(players):
    for player in players:
        if 'BirthDate' in player and player['BirthDate']:
            player['BirthDate'] = pd.to_datetime(player['BirthDate'], format='%Y-%m-%dT%H:%M:%S', errors='coerce').strftime('%Y-%m-%d')
        if 'ProDebut' in player and player['ProDebut']:
            player['ProDebut'] = pd.to_datetime(player['ProDebut'], format='%Y-%m-%dT%H:%M:%S', errors='coerce').strftime('%Y-%m-%d')
    return players


def store_players_in_csv(players, csv_file_path):
    df = pd.DataFrame(players)
    df.to_csv(csv_file_path, index=False)

def ingest_marlins_roster():
    api_key = os.getenv('SPORTS_DATA_API_KEY')
    team = 'MIA'
    players = fetch_team_roster(team, api_key)
    players = convert_datetime_format(players)
    csv_file_path = os.getenv('RAW_DATA_PATH')
    store_players_in_csv(players, csv_file_path)

    # Print the first few rows of the DataFrame to inspect the data
    print(pd.DataFrame(players).head())

# Example usage
if __name__ == "__main__":
    ingest_marlins_roster()