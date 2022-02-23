import base64
import datetime
import pandas as pd
import sys

from sentry_sdk import init
from google.cloud import bigquery

from utils import pubsub


def validate_txns(data):
    """
    Run transaction verification for the given date.
    Args:
        data (dict): Event payload from Google Cloud Function.
        contains the Pub/Sub message in the format {"env":"<env_name>"}
    """
    p = pubsub.decode_payload(data)