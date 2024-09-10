BEGIN;

WITH spent_expired_transactions AS (
  SELECT trans_id,
         customerid,
         createdat,
         ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY createdat ASC) AS rn
  FROM tc_data
  WHERE tctype IN ('spent', 'expired')
  ),
earned_transactions AS (
  SELECT trans_id,
         customerid,
         createdat,
         ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY createdat ASC) AS rn
  FROM tc_data
  WHERE tctype = 'earned'
  ),
transactions_to_apply AS (
  SELECT se.trans_id AS spent_expired_trans_id,
         e.trans_id AS earned_trans_id
  FROM spent_expired_transactions se
  JOIN earned_transactions e
  USING (customerid, rn)
  )
UPDATE tc_data tc
SET redeemid = tta.spent_expired_trans_id
FROM transactions_to_apply AS tta
WHERE tc.trans_id = tta.earned_trans_id
AND tc.tctype = 'earned';

COMMIT;
