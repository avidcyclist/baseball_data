import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the cleaned data
file_path = 'C:/Users/Mitch/Desktop/data-engineering-project/data/processed/cleaned_marlins_roster.csv'
cleaned_data = pd.read_csv(file_path)

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