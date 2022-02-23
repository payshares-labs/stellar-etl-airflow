import base64
import json

def decode_payload(data):
    """Decodes base64 payloads from Pub/Sub topics.
       User can supply the full payload from GCP and
       the message will be decoded and returned as a
       JSON object
    Args:
        data (dict): Pub/Sub context returned from GCP when
            executing a GCP Cloud Function.
    Returns:
        object: decoded JSON object that contains the
            message from Pub/Sub
    """
    encoded_data = data["data"]
    decoded_data = base64.b64decode(encoded_data)

    return json.loads(decoded_data)