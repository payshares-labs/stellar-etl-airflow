import datetime
import json
import logging

from sentry_sdk import init, capture_message
from google.cloud import bigquery

import config
from utils import bq, pubsub

def validate_ledgers(data):
    """
    Run ledger verification for the given date.
    Args:
        data (dict): Event payload from Google Cloud Function.
        contains the Pub/Sub message in the format {"env":"<env_name>"}
    """
    p = pubsub.decode_payload(data)
    env = p["env"]
    logging.info(f"Running ledger check for environment: {env}")

    if config.SENTRY_ON:
        init(config.SENTRY_DSN_HUBBLE, environment=env)

    result = bq.do_query(config.LEDGERS_BY_SEQUENCE_QUERY, env=env)
    num_missing = result.total_rows
    if num_missing == 0:
        # no sequences missing
        return

    missing = [r.missing_sequence for r in result]
    logging.error(f"missing these sequences: {missing}")

    if config.SENTRY_ON:
        logging.info("Sending sentry error...")
        display_missing = missing[0:math.min(5, len(missing))]
        capture_message(f"missing {num_missing} sequences from ledgers including: {display_missing}; check logs for full list")

    result = bq.do_query(config.LEDGERS_BY_BATCH_ID_QUERY, env=env)
    num_missing = result.total_rows
    if num_missing == 0:
        logging.error("no batches found")
        return

    batches = set()
    for r in result:
        batches.add(r.batch_id)
    logging.error(f"rerun {len(batches)} batches: {batches}")

    if config.SENTRY_ON:
        logging.info("Sending sentry error...")
        display_missing = batches[0:math.min(5, len(batches))]
        capture_message(f"missing {len(batches)} batches from ledgers including: {display_missing}; check logs for full list")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, help="Environment name for script execution", default="dev")
    args = parser.parse_args()

    data = {"@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
            "attributes": None,
            "data": "eyJ0YXJnZXQiOiJ0ZXN0In0="}

    validate_ledgers(data=data)