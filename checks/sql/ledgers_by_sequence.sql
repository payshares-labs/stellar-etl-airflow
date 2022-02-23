--Get the latest sequence available in Hubble so that we know the end bound
with max_ledger as (
    SELECT max(sequence) as max_sequence
    from `{project_id}.{dataset_id}.history_ledgers`
)
SELECT DISTINCT sequence + 1 as missing_sequence
FROM `{project_id}.{dataset_id}.history_ledgers`
WHERE sequence + 1 NOT IN (SELECT DISTINCT sequence FROM `{project_id}.{dataset_id}.history_ledgers`)
  and sequence < (SELECT max_sequence from max_ledger)
order by missing_sequence