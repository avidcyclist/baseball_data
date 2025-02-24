import sys
import os

# Add the root directory of the project to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
from sqlalchemy import create_engine, inspect
from src.utils.db_utils import get_db_connection
from dotenv import load_dotenv

load_dotenv()

def create_players_table(engine):
    """
    Create the players table with the correct schema if it doesn't exist.
    
    Parameters:
    engine (Engine): SQLAlchemy engine object.
    """
    with engine.connect() as connection:
        connection.execute('''
            CREATE TABLE IF NOT EXISTS players (
                 operation_type TEXT NOT NULL,
                operation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
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
        
        
def load_data_to_players_db(transformed_data, db_path, table_name):
    """
    Load transformed data into the specified SQLite database table.

    Parameters:
    transformed_data (DataFrame): The data to be loaded into the database.
    db_path (str): Path to the SQLite database file.
    table_name (str): Name of the target table in the database.
    """
    # Establish a database connection
    engine = create_engine(f'sqlite:///{db_path}')
    inspector = inspect(engine)

    # Check if the table exists
    if inspector.has_table(table_name):
        # Load existing data from the table
        existing_data = pd.read_sql(table_name, con=engine)

        # Identify new records by checking which IDs are not in the existing data
        new_records = transformed_data[~transformed_data['PlayerID'].isin(existing_data['PlayerID'])]

        if not new_records.empty:
            # Append new records to the table
            new_records.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Inserted {len(new_records)} new records into the '{table_name}' table.")
        else:
            print(f"No new records to insert into the '{table_name}' table.")
    else:
        # If the table doesn't exist, create it and insert all data
        transformed_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' created successfully with {len(transformed_data)} entries.")

    
def create_audit_table(engine):
    """
    Create the players_audit table if it does not exist.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS players_audit (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation_type TEXT NOT NULL,
        operation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        PlayerID INTEGER,
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
    );
    """
    with engine.connect() as connection:
        connection.execute(create_table_sql)

# Example usage
if __name__ == "__main__":
    # Read data from CSV file
    csv_file_path = 'C:/Users/Mitch/Desktop/data-engineering-project/data/processed/cleaned_all_active_players.csv'
    transformed_data = pd.read_csv(csv_file_path, parse_dates=['BirthDate', 'ProDebut'])
    
    # Load data into the new database
    db_path = os.getenv('DB_PATH_PLAYERS')
    load_data_to_players_db(transformed_data, db_path, table_name='players')