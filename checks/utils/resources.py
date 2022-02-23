import config
import os

def get_query_filepath(query_name):
    root = os.path.join(os.path.dirname(__file__), '..')
    return os.path.join(root, f'{config.SQL_DIR}/{query_name}.sql')