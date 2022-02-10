"""
Function called by PubSub trigger to execute cron job tasks for veryifing data output by setllar-etl
"""
import argparse
import logging
import sys

import functions.verify_ledgers as verify_ledgers
import functions.verify_txns as verify_txns
import functions.verify_operations as verify_operations

if __name__ == "__main__":
    """
    All Cloud Functions can be tested locally by passing by passing the
    function name, env, and when needed, run date.
    The argparser allows the developer to pass the env (defaults to "test")
    When testing locally, pass "test" to the script or do not pass any args.
    Production failures can be handled by passing "production" and setting the "--unsafe" flag.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, help="Cloud Function name to execute")
    parser.add_argument("--env", type=str, help="Environment name for script execution", default="test")
    parser.add_argument("--unsafe", help="Overrides the script exit that prevents user from running in prod", \
                        default=False, action="store_true")
    parser.add_argument("--date", type=str, help="Date in format YYYY-MM-DD to represent run date.")
    args = parser.parse_args()

    # set up logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    root.addHandler(handler)

    if args.env == "production":
        logging.warning(f"You are trying to run the {args.name} function in prod! \
                         Please keep your local testing to the test env.")
        if not args.unsafe:
            sys.exit("Override not provided. Script exiting now...")

    sample_message = {"production": {"@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
                               "attributes": None,
                               "data": "eyJ0YXJnZXQiOiJwcm9kdWN0aW9uIn0="},
                      "test": {"@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
                               "attributes": None,
                               "data": "eyJ0YXJnZXQiOiJ0ZXN0In0="}}

    logging.debug(f"Running the {args.name} script in the {args.env} environment")
    data = sample_message[args.env]
    if args.name == "veryify_ledgers":
        verify_ledgers(data=data, context="")
    elif args.name == "veryify_txns":
        verify_txns(data=data, context="")
    elif args.name == "veryify_operations":
        verify_operations(data=data, context="")
    else:
        logging.warning(f"Function name {args.name} is invalid! Please pass a valid name.")
