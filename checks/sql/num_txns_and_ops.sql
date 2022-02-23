-- Get the actual count of transactions per ledger
WITH txn_count AS (
    SELECT ledger_sequence, COUNT(id) as txn_transaction_count
    FROM `{project_id}.{dataset_id}.history_transactions`
    GROUP BY ledger_sequence
),
-- Get the actual count of operations per ledger
     operation_count AS (
         SELECT A.ledger_sequence, COUNT(B.id) AS op_operation_count
         FROM `{project_id}.{dataset_id}.history_transactions` A
                  JOIN `{project_id}.{dataset_id}.history_operations` B
                       ON A.id = B.transaction_id
         GROUP BY A.ledger_sequence
     ),
-- compare actual counts with the counts reported in the ledgers table
     final_counts AS (
         SELECT A.sequence, A.closed_at, A.batch_id,
                A.tx_set_operation_count as expected_operation_count,
                A.operation_count,
                (A.failed_transaction_count + A.successful_transaction_count) as expected_transaction_count,
                COALESCE(B.txn_transaction_count, 0) as actual_transaction_count,
                COALESCE(C.op_operation_count, 0) as actual_operation_count
         FROM `{project_id}.{dataset_id}.history_ledgers` A
                  LEFT OUTER JOIN txn_count B
                                  ON A.sequence = B.ledger_sequence
                  LEFT OUTER JOIN operation_count C
                                  ON A.sequence = C.ledger_sequence
     )
        , raw_values AS (
    SELECT sequence, closed_at, batch_id,
           expected_transaction_count, actual_transaction_count,
           expected_operation_count, actual_operation_count
    FROM final_counts
    WHERE
        ((expected_transaction_count <> actual_transaction_count)
            OR (expected_operation_count <> actual_operation_count))
)
SELECT batch_id,
       SUM(expected_transaction_count) as exp_txn_count,
       SUM(actual_transaction_count ) as actual_txn_count,
       SUM(expected_operation_count ) as exp_op_count,
       SUM(actual_operation_count ) as actual_operation_count
FROM raw_values
--@TODO: figure out a more precise delay for ledgers. Since tables are loaded on a 15-30 min delay,
-- we do not want a premature alert to row count mismatches when it could be loading latency
WHERE closed_at <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -60 MINUTE)
GROUP BY batch_id
ORDER BY batch_id