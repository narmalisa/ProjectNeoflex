SELECT client_rk, effective_from_date, COUNT(*) AS количество
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1;



BEGIN;

WITH RankedDuplicates AS (
    SELECT 
        ctid,  
        ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date ORDER BY ctid) AS rn
    FROM 
        dm.client
)
DELETE FROM dm.client
WHERE ctid IN (
    SELECT ctid
    FROM RankedDuplicates
    WHERE rn > 1
);

COMMIT;

ROLLBACK;
