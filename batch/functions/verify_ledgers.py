import config
import datetime
import pandas as pd
import sys

from sentry_sdk import init
from utils import bq


def verify_ledgers(data):
    """
    Run ledger verification for the given date.
    Args:
        data (dict): Event payload from Google Cloud Function.
        contains the Pub/Sub message in the format {"target":"<env_name>"}

    """
    env = helpers.decode_payload(data)["target"]
    if env=="production":
        init(config.SENTRY_DSN_HUBBLE_ENG)

    # get expected range
    # read ledgers from bq
    # ensure the entire range is present