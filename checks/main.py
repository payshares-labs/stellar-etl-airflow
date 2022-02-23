import argparse
import logging
import sys

from functions import validate_ledgers as vl
from functions import validate_txns as vt

def validate_ledgers(data, context):
    """Validates all ledger sequences are present, and any missing batch_ids if there are."""
    vl.validate_ledgers(data)

def validate_txns(data, context):
    """Validates #txns and #ops written per ledger is consistent with #txns and #ops in ledger entry."""
    vt.validate_txns(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, help="Cloud Function name to execute")
    parser.add_argument("--env", type=str, help="Environment name for script execution", default="dev")
    args = parser.parse_args()

    # set up logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    root.addHandler(handler)

    data = {
        "@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
        "attributes": None,
        "data": "eyJlbnYiOiJkZXZlbG9wbWVudCJ9Cg=="}  # base64 encoded {"env":"development"}

    logging.debug(f"Running the {args.name} script in the {args.env} environment")
    if args.name == "validate_ledgers":
        validate_ledgers(data=data, context="")
    elif args.name == "validate_txns":
        validate_txns(data=data, context="")
    else:
        logging.warning(f"Function name {args.name} is invalid! Please pass a valid name.")
