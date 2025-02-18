import os
import sys

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transform.transform_data import transform_data
from src.load.load_data import load_data_to_db

def run_etl():
    # List of file paths to process
    file_paths = [
        os.getenv('CLEANED_DATA_PATH'),
        os.getenv('CLEANED_PLAYERS_DATA_PATH')
    ]

    # Step 1: Transform data
    transformed_data_list = transform_data(file_paths)

    # Step 2: Load data into the database
    db_configs = [
        {'db_path': os.getenv('DB_PATH'), 'table_name': 'marlins_players'},
        {'db_path': os.getenv('DB_PATH_PLAYERS'), 'table_name': 'players'}
    ]

    for transformed_data, db_config in zip(transformed_data_list, db_configs):
        load_data_to_db(transformed_data, db_config['db_path'], db_config['table_name'])

if __name__ == "__main__":
    run_etl()
