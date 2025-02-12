import os
import sys

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transform.transform_data import transform_data
from src.load.load_data import load_data_to_db

def run_etl():
    # Step 1: Load raw data from CSV
    file_path = os.getenv('RAW_DATA_PATH')
    
    # Step 2: Transform data
    transformed_data = transform_data(file_path)
    
    # Step 3: Load data into the database
    db_config = {
        'database': os.getenv('DB_PATH')
    }
    load_data_to_db(transformed_data, db_config)

if __name__ == "__main__":
    run_etl()