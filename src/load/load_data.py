import sys
import os

# Add the root directory of the project to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
from sqlalchemy import create_engine, inspect
from src.utils.db_utils import get_db_connection
from datetime import datetime

# Enable Copy-on-Write mode
pd.options.mode.copy_on_write = True

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
                ProDebut TEXT
            )
        ''')
        
def create_audit_table(db_path):
    """
    Create the players_audit table to log all changes.
    
    Parameters:
    db_path (str): Path to the SQLite database file.
    """
    engine = create_engine(f'sqlite:///{db_path}')
    with engine.connect() as connection:
        connection.execute('''
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
                ProDebut TEXT
            )
        ''')

def load_data_to_db(transformed_data, db_path, table_name):
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

    # Add a timestamp column to the transformed data
    transformed_data['operation_timestamp'] = datetime.now()

    # Check if the table exists
    if inspector.has_table(table_name):
        # Load existing data from the table
        existing_data = pd.read_sql(table_name, con=engine)

        # Identify new records by checking which IDs are not in the existing data
        new_records = transformed_data[~transformed_data['PlayerID'].isin(existing_data['PlayerID'])]

        # Identify updated records by checking for differences in existing records
        common_records = transformed_data[transformed_data['PlayerID'].isin(existing_data['PlayerID'])]
        updated_records = common_records.merge(existing_data, on='PlayerID', suffixes=('', '_existing'))

        # Ensure matching columns before comparison
        updated_records_filtered = updated_records.filter(regex='^(?!.*_existing$)')
        common_records_filtered = common_records[updated_records_filtered.columns]

        if not new_records.empty:
            # Append new records to the table
            new_records.to_sql(table_name, con=engine, if_exists='append', index=False)
            new_records.loc[:,'operation_type'] = 'insert'
            new_records.to_sql('players_audit', con=engine, if_exists='append', index=False)
            print(f"Inserted {len(new_records)} new records into the '{table_name}' table.")

        if not updated_records_filtered.empty:
            # Update existing records in the table
            updated_records_filtered.to_sql(table_name, con=engine, if_exists='replace', index=False)
            updated_records_filtered = updated_records_filtered.copy()
            updated_records_filtered.loc[:, 'operation_type'] = 'update'
            updated_records_filtered.to_sql('players_audit', con=engine, if_exists='append', index=False)
            print(f"Updated {len(updated_records_filtered)} records in the '{table_name}' table.")

        # Identify deleted records by checking which IDs are not in the transformed data
        deleted_records = existing_data[~existing_data['PlayerID'].isin(transformed_data['PlayerID'])]
        if not deleted_records.empty:
            deleted_records = deleted_records.copy()
            deleted_records.loc[:, 'operation_type'] = 'delete'
            deleted_records.to_sql('players_audit', con=engine, if_exists='append', index=False)
            print(f"Deleted {len(deleted_records)} records from the '{table_name}' table.")
    else:
        # If the table doesn't exist, create it and insert all data
        transformed_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        transformed_data.loc[:, 'operation_type'] = 'insert'
        transformed_data.to_sql('players_audit', con=engine, if_exists='append', index=False)
        print(f"Table '{table_name}' created successfully with {len(transformed_data)} entries.")
        
    # Filter active players
    active_players = transformed_data[transformed_data['Status'] == 'Active']

   # Create or update the active_players table
    if inspector.has_table('active_players'):
        # Load existing data from the active_players table
        existing_active_data = pd.read_sql('active_players', con=engine)

        # Identify new active records
        new_active_records = active_players[~active_players['PlayerID'].isin(existing_active_data['PlayerID'])]

        # Identify updated active records
        common_active_records = active_players[active_players['PlayerID'].isin(existing_active_data['PlayerID'])]
        updated_active_records = common_active_records.merge(existing_active_data, on='PlayerID', suffixes=('', '_existing'))

        # Ensure matching columns before comparison
        updated_active_records_filtered = updated_active_records.filter(regex='^(?!.*_existing$)')
        common_active_records_filtered = common_active_records[updated_active_records_filtered.columns]

        if not new_active_records.empty:
            # Append new active records to the active_players table
            new_active_records.to_sql('active_players', con=engine, if_exists='append', index=False)
            new_active_records.loc[:,'operation_type'] = 'insert'
            new_active_records.to_sql('active_players_audit', con=engine, if_exists='append', index=False)
            print(f"Inserted {len(new_active_records)} new records into the 'active_players' table.")

        if not updated_active_records_filtered.empty:
            # Update existing active records in the active_players table
            for index, row in updated_active_records_filtered.iterrows():
                # Convert row to a dictionary and handle None values
                row_dict = row.to_dict()
                row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
                # Convert Timestamp to string
                for key in ['BirthDate', 'ProDebut', 'operation_timestamp']:
                    if key in row_dict and isinstance(row_dict[key], pd.Timestamp):
                        row_dict[key] = row_dict[key].strftime('%Y-%m-%d %H:%M:%S')
                engine.execute(f"""
                    UPDATE active_players
                    SET {', '.join([f"{col} = ?" for col in updated_active_records_filtered.columns])}
                    WHERE PlayerID = ?
                """, tuple(row_dict.values()) + (row_dict['PlayerID'],))
            updated_active_records_filtered['operation_type'] = 'update'
            updated_active_records_filtered.to_sql('active_players_audit', con=engine, if_exists='append', index=False)
            print(f"Updated {len(updated_active_records_filtered)} records in the 'active_players' table.")

        # Identify deleted active records
        deleted_active_records = existing_active_data[~existing_active_data['PlayerID'].isin(active_players['PlayerID'])]
        if not deleted_active_records.empty:
            deleted_active_records = deleted_active_records.copy()
            deleted_active_records.loc[:, 'operation_type'] = 'delete'
            deleted_active_records.to_sql('active_players_audit', con=engine, if_exists='append', index=False)
            engine.execute(f"DELETE FROM active_players WHERE PlayerID IN ({', '.join(map(str, deleted_active_records['PlayerID'].tolist()))})")
            print(f"Deleted {len(deleted_active_records)} records from the 'active_players' table.")
    else:
        # If the active_players table doesn't exist, create it and insert all active data
        active_players.to_sql('active_players', con=engine, if_exists='replace', index=False)
        active_players.loc[:,'operation_type'] = 'insert'
        active_players.to_sql('active_players_audit', con=engine, if_exists='append', index=False)
        print(f"Table 'active_players' created successfully with {len(active_players)} entries.")


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
    # Database configuration
    db_path = 'C:/Users/Mitch/Desktop/data-engineering-project/src/ingest/marlins_roster.db'
    table_name = 'marlins_players'
    
    # Load data into the database
    load_data_to_db(transformed_data, db_path, table_name)