import pandas as pd
from sqlalchemy import create_engine
from src.utils.db_utils import get_db_connection

def load_data_to_db(transformed_data, db_config):
    """
    Load transformed data into the specified database.
    
    Parameters:
    transformed_data (DataFrame): The data to be loaded into the database.
    db_config (dict): Database configuration containing connection details.
    """
    # Establish a database connection
    engine = get_db_connection(db_config)
    
    # Load existing data from the database
    existing_data = pd.read_sql('baseball_data', con=engine)
    
    # Find new entries by comparing the transformed data with the existing data
    new_entries = transformed_data[~transformed_data['PlayerID'].isin(existing_data['PlayerID'])]
    
    # Append new entries to the database
    if not new_entries.empty:
        new_entries.to_sql('baseball_data', con=engine, if_exists='append', index=False)
        print(f"Data loaded successfully into the database. {len(new_entries)} new entries added.")
    else:
        print("No new entries to add.")
