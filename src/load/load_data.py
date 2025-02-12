# filepath: /c:/Users/Mitch/Desktop/data-engineering-project/src/load/load_data.py
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
    
    # Load data into the database
    transformed_data.to_sql('baseball_data', con=engine, if_exists='replace', index=False)
    
    print("Data loaded successfully into the database.")