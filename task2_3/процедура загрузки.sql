CREATE OR REPLACE PROCEDURE reload_account_balance_turnover()
LANGUAGE plpgsql AS $$
BEGIN
    -- Удаление старых данных из витрины
    DELETE FROM dm.account_balance_turnover;

    -- Вставка новых данных с исправлениями
    INSERT INTO dm.account_balance_turnover (account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
   SELECT a.account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
ORDER BY a.account_rk,  ab.effective_date,  ab.account_in_sum ;
END;
$$
;

call reload_account_balance_turnover()

select * from dm.account_balance_turnover 