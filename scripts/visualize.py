import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import os

load_dotenv()
# Load the cleaned data
file_path = os.getenv('CLEANED_DATA_PATH')
cleaned_data = pd.read_csv(file_path)
all_players_file_path = os.getenv('ALL_PLAYERS_CSV_PATH')
all_players_data = pd.read_csv(all_players_file_path)
# Example visualization: Distribution of player heights
plt.figure(figsize=(10, 6))
sns.histplot(cleaned_data['Height'], bins=20, kde=True)
plt.title('Distribution of Player Heights')
plt.xlabel('Height (inches)')
plt.ylabel('Frequency')
plt.show()

# Example visualization: Distribution of player weights
plt.figure(figsize=(10, 6))
sns.histplot(cleaned_data['Weight'], bins=20, kde=True)
plt.title('Distribution of Player Weights')
plt.xlabel('Weight (lbs)')
plt.ylabel('Frequency')
plt.show()

# Example visualization: Count of players by position category
plt.figure(figsize=(10, 6))
sns.countplot(data=cleaned_data, x='PositionCategory', order=cleaned_data['PositionCategory'].value_counts().index)
plt.title('Count of Players by Position Category')
plt.xlabel('Position Category')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.show()

# Example visualization: Count of players by birth state (USA only)
usa_players = cleaned_data[cleaned_data['BirthCountry'] == 'USA']
plt.figure(figsize=(12, 8))
sns.countplot(data=usa_players, y='BirthState', order=usa_players['BirthState'].value_counts().index)
plt.title('Count of Players by Birth State (USA)')
plt.xlabel('Count')
plt.ylabel('Birth State')
plt.show()