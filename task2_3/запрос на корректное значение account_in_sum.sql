WITH corrected_values AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        ab1.account_in_sum,
        ab2.account_out_sum AS previous_account_out_sum
    FROM 
        rd.account_balance ab1
    LEFT JOIN 
        rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
        AND ab1.effective_date = ab2.effective_date + INTERVAL '1 day'
)

SELECT 
    account_rk,
    effective_date,
    CASE 
        WHEN account_in_sum <> previous_account_out_sum THEN previous_account_out_sum 
        ELSE account_in_sum 
    END AS corrected_account_in_sum
FROM 
    corrected_values;