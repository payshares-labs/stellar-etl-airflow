--Identify the ledger edges for a batch
WITH ledger_ranges as (
    select l.batch_id, MIN(sequence) as start_sequence, MAX(sequence) as end_sequence
    FROM `{project_id}.{dataset_id}.history_ledgers` l
    GROUP BY l.batch_id
),
--flatten the results so that the proceeded batch start sequence aligns with the prior batch
     get_next as (
         SELECT batch_id, end_sequence, LEAD(start_sequence) OVER (ORDER BY batch_id ASC) as next_start_sequence
         FROM ledger_ranges
         ORDER BY batch_id
     )
SELECT batch_id, next_start_sequence - end_sequence as missing_ledgers
FROM get_next
--If there is more than 1 sequence gap between a batch end
-- and the next batch start, we know that there was a partial load
WHERE next_start_sequence - end_sequence > 1
ORDER BY batch_id