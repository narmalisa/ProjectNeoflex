WITH corrected_values AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        ab1.account_out_sum,
        ab2.account_in_sum AS current_account_in_sum
    FROM 
        rd.account_balance ab1
    LEFT JOIN 
        rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
        AND ab1.effective_date + INTERVAL '1 day' = ab2.effective_date
)
SELECT 
    account_rk,
    effective_date,
    CASE 
        WHEN account_out_sum <> current_account_in_sum THEN current_account_in_sum 
        ELSE account_out_sum 
    END AS corrected_account_out_sum
FROM 
    corrected_values;
