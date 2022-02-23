# env
PROD = "production"
DEV = "development"

# resource directory tree
SQL_DIR = "sql"

# GCP / BigQuery
PROJECT_ID = {
    PROD: "hubble-261722",
    DEV: "test-hubble-319619"
  }

DATASET_ID = {
    PROD: "crypto_stellar_internal_2",
    DEV: "test_gcp_airflow_internal"
  }

LEDGERS_BY_SEQUENCE_QUERY = "ledgers_by_sequence"
LEDGERS_BY_BATCH_ID_QUERY = "ledgers_by_batch_id"
NUM_TXNS_AND_OPS_QUERY = "num_txns_and_ops"

# Sentry
SENTRY_ON = False
SENTRY_DSN_HUBBLE = "https://9e0a056541c3445083329b072f2df690@o14203.ingest.sentry.io/6190849"