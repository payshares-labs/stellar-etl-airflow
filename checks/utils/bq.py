import config
import datetime
import logging
from utils import resources

from google.cloud import bigquery
from string import Template

def do_query(query_name, env=config.PROD):
    """Runs the query with the given name.
    Args:
        query_name (string): Query to run.
    """
    current_time = datetime.datetime.utcnow()
    log_message = f"Cloud function {query_name} was triggered on {current_time}"
    logging.info(log_message)

    #query_filepath = resources.get_query_filepath(config.LEDGERS_BY_SEQUENCE_QUERY)
    #query = bq.file_to_string(query_filepath)

    try:
        return execute_query(query_name=query_name, env=env)
    except Exception as error:
        log_message = Template("Query failed due to:\n$message.")
        logging.error(log_message.safe_substitute(query_name=query_name, message=error))

def execute_query(query_name=None, query=None, env=config.PROD):
    """Executes transformation query to a new destination table.
    Args:
        query_name (string): Output table and SQL filename (without extension).
        query (string): SQL query to execute.
        env (string): environment name to execute the query.
    """
    if (query_name is None) and (query is None):
        logging.error("At least one of query_name or query must be defined.")
        return

    bq_client = bigquery.Client()
    dataset_ref = bq_client.get_dataset(bigquery.DatasetReference(
        project=config.PROJECT_ID[env],
        dataset_id=config.DATASET_ID[env]
    ))
    sql_params = {
        'project_id': config.PROJECT_ID[env],
        'dataset_id': config.DATASET_ID[env]
      }

    job_config = bigquery.QueryJobConfig()

    if query_name:
        # Retrieve query string.
        if not query:
            db_fn = resources.get_query_filepath(query_name)#f"{config.SQL_DIR}/{query_name}.sql"
            sql = file_to_string(db_fn)

        # Configure job.
        job_config.destination = dataset_ref.table(query_name)
        job_config.write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE
        logging.info(f"Attempting query {query_name}...")

    if query:
        sql = query
        logging.info("Attempting provided query...")

    # Execute query.
    logging.debug(sql.format(**sql_params))
    query_job = bq_client.query(sql.format(**sql_params), job_config=job_config)
    result = query_job.result() # Wait for query to finish.
    logging.info("Query complete.")

    return result

def file_to_string(sql_path):
    """Converts a SQL file with a SQL query to a string.
    Args:
        sql_path: String containing a file path
    Returns:
        String representation of a file's contents
    """
    with open(sql_path, "r") as sql_file:
        return sql_file.read()
