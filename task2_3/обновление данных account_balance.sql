WITH corrected_values AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        CASE 
            WHEN ab1.account_in_sum <> LAG(ab1.account_out_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date) THEN LAG(ab1.account_out_sum) OVER (PARTITION BY ab1.account_rk ORDER BY ab1.effective_date) 
            ELSE ab1.account_in_sum 
        END AS new_account_in_sum
    FROM 
        rd.account_balance ab1
)
UPDATE rd.account_balance
SET account_in_sum = cv.new_account_in_sum
FROM corrected_values cv
WHERE rd.account_balance.account_rk = cv.account_rk AND rd.account_balance.effective_date = cv.effective_date;

