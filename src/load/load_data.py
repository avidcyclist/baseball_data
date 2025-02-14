import sys
import os

# Add the root directory of the project to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
from sqlalchemy import create_engine, inspect
from src.utils.db_utils import get_db_connection

def create_marlins_players(engine):
    """
    Create the marlins_players table with the correct schema if it doesn't exist.
    
    Parameters:
    engine (Engine): SQLAlchemy engine object.
    """
    with engine.connect() as connection:
        connection.execute('''
            CREATE TABLE IF NOT EXISTS marlins_players (
                PlayerID INTEGER PRIMARY KEY,
                SportsDataID TEXT,
                Status TEXT,
                TeamID INTEGER,
                Team TEXT,
                Jersey INTEGER,
                PositionCategory TEXT,
                Position TEXT,
                MLBAMID INTEGER,
                FirstName TEXT,
                LastName TEXT,
                BatHand TEXT,
                ThrowHand TEXT,
                Height REAL,
                Weight REAL,
                BirthDate TEXT,
                BirthCity TEXT,
                BirthState TEXT,
                BirthCountry TEXT,
                HighSchool TEXT,
                College TEXT,
                ProDebut TEXT,
                Salary REAL,
                PhotoUrl TEXT,
                SportRadarPlayerID TEXT,
                RotoworldPlayerID INTEGER,
                RotoWirePlayerID INTEGER,
                FantasyAlarmPlayerID INTEGER,
                StatsPlayerID INTEGER,
                SportsDirectPlayerID INTEGER,
                XmlTeamPlayerID INTEGER,
                InjuryStatus TEXT,
                InjuryBodyPart TEXT,
                InjuryStartDate TEXT,
                InjuryNotes TEXT,
                FanDuelPlayerID INTEGER,
                DraftKingsPlayerID INTEGER,
                YahooPlayerID INTEGER,
                UpcomingGameID INTEGER,
                FanDuelName TEXT,
                DraftKingsName TEXT,
                YahooName TEXT,
                GlobalTeamID INTEGER,
                FantasyDraftName TEXT,
                FantasyDraftPlayerID INTEGER,
                Experience INTEGER,
                UsaTodayPlayerID INTEGER,
                UsaTodayHeadshotUrl TEXT,
                UsaTodayHeadshotNoBackgroundUrl TEXT,
                UsaTodayHeadshotUpdated TEXT,
                UsaTodayHeadshotNoBackgroundUpdated TEXT
            )
        ''')

def load_data_to_db(transformed_data, db_config):
    """
    Load transformed data into the specified database.
    
    Parameters:
    transformed_data (DataFrame): The data to be loaded into the database.
    db_config (dict): Database configuration containing connection details.
    """
    # Establish a database connection
    engine = get_db_connection(db_config)
    inspector = inspect(engine)
    # Create the table with the correct schema
    create_marlins_players(engine)
    
    # Load the transformed data into the database
    transformed_data.to_sql('marlins_players', con=engine, if_exists='replace', index=False)
    print(f"Table 'marlins_players' created successfully with {len(transformed_data)} entries.")

# Example usage
if __name__ == "__main__":
    # Read data from CSV file
    csv_file_path = 'C:/Users/Mitch/Desktop/data-engineering-project/data/raw/marlins_roster.csv'
    transformed_data = pd.read_csv(csv_file_path, parse_dates=['BirthDate'])
    
    # Database configuration
    db_config = {
        'dialect': 'sqlite',
        'database': 'C:/Users/Mitch/Desktop/data-engineering-project/src/ingest/marlins_roster.db'
    }
    
    # Load data into the database
    load_data_to_db(transformed_data, db_config)