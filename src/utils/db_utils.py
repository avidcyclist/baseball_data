# filepath: /c:/Users/Mitch/Desktop/data-engineering-project/src/utils/db_utils.py
from sqlalchemy import create_engine

def get_db_connection(db_config):
    """
    Create a database connection using the provided configuration.
    
    Parameters:
    db_config (dict): Database configuration containing connection details.
    
    Returns:
    engine: SQLAlchemy engine object.
    """
    database = db_config['database']
    
    connection_string = f"sqlite:///{database}"
    engine = create_engine(connection_string)
    
    return engine

def execute_query(engine, query):
    with engine.connect() as connection:
        result = connection.execute(query)
        return result

def close_connection(engine):
    engine.dispose()

def handle_error(error):
    print(f"An error occurred: {error}")