CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f(IN i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id INT;
    v_FromDate DATE := DATE_TRUNC('month', i_OnDate) - INTERVAL '1 MONTH'; -- последний день предыдущего месяца
    v_ToDate DATE := (DATE_TRUNC('month', v_FromDate) + INTERVAL '1 month') - INTERVAL '1 day'; -- последний день отчетного периода
BEGIN 
    -- Логирование начала работы
    INSERT INTO "LOGS".turnover_log (start_message, start_time)
    VALUES ('Начало заполнения 101 формы за ' || i_OnDate::TEXT, now())
    RETURNING log_id INTO v_log_id;  -- Получаем идентификатор новой записи

    -- Удаляем старые записи за отчетную дату
    DELETE FROM "DM".DM_F101_ROUND_F WHERE FROM_DATE =  v_FromDate;

    -- Заполнение витрины данными
    INSERT INTO "DM".DM_F101_ROUND_F (
        FROM_DATE,
        TO_DATE,
        CHAPTER,
        LEDGER_ACCOUNT,
        CHARACTERISTIC,
        BALANCE_IN_RUB,
        BALANCE_IN_VAL,
        BALANCE_IN_TOTAL,
        TURN_DEB_RUB,
        TURN_DEB_VAL,
        TURN_DEB_TOTAL,
        TURN_CRE_RUB,
        TURN_CRE_VAL,
        TURN_CRE_TOTAL,
        BALANCE_OUT_RUB,
        BALANCE_OUT_VAL,
        BALANCE_OUT_TOTAL
    )
    SELECT 
        v_FromDate AS FROM_DATE,
        v_ToDate AS TO_DATE, 
        "DS"."MD_LEDGER_ACCOUNT_S"."CHAPTER" AS CHAPTER,
        SUBSTRING("DS"."MD_ACCOUNT_D"."ACCOUNT_NUMBER", 1, 5) AS LEDGER_ACCOUNT,
        "DS"."MD_ACCOUNT_D"."CHAR_TYPE" AS CHARACTERSTIC,

        -- Остатки на день перед отчетным периодом (рублевые счета)
        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" IN ('810', '643') THEN 
            balance_rub.BALANCE_OUT_RUB
            ELSE 0 END) AS BALANCE_IN_RUB,

        -- Остатки на день перед отчетным периодом (все кроме рублевых)
        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" NOT IN ('810', '643') THEN 
            balance_rub.BALANCE_OUT_RUB
            ELSE 0 END) AS BALANCE_IN_VAL,

        -- Остатки на день перед отчетным периодом (все)
        SUM(balance_rub.BALANCE_OUT_RUB) AS BALANCE_IN_TOTAL,

        -- Дебетовые обороты за отчетный период
        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" IN ('810', '643') THEN 
            turnover.debet_amount_rub ELSE 0 END) AS TURN_DEB_RUB,

        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" NOT IN ('810', '643') THEN 
            turnover.debet_amount_rub ELSE 0 END) AS TURN_DEB_VAL,

        SUM(turnover.debet_amount_rub) AS TURN_DEB_TOTAL,

        -- Кредитовые обороты за отчетный период
        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" IN ('810', '643') THEN 
            turnover.credit_amount_rub ELSE 0 END) AS TURN_CRE_RUB,

        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" NOT IN ('810', '643') THEN 
            turnover.credit_amount_rub ELSE 0 END) AS TURN_CRE_VAL,

        SUM(turnover.credit_amount_rub) AS TURN_CRE_TOTAL,

        -- Остатки на последний день отчетного периода (рублевые счета)
        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" IN ('810', '643') THEN 
            (SELECT BALANCE_OUT_RUB FROM "DM".DM_ACCOUNT_BALANCE_F WHERE ON_DATE = v_ToDate 
            AND ACCOUNT_RK = balance_rub.ACCOUNT_RK)
            ELSE 0 END) AS BALANCE_OUT_RUB,

        -- Остатки на последний день отчетного периода (все кроме рублевых)
        SUM(CASE WHEN "DS"."MD_ACCOUNT_D"."CURRENCY_CODE" NOT IN ('810', '643') THEN 
            (SELECT BALANCE_OUT_RUB FROM "DM".DM_ACCOUNT_BALANCE_F WHERE ON_DATE = v_ToDate
            AND ACCOUNT_RK = balance_rub.ACCOUNT_RK)
            ELSE 0 END) AS BALANCE_OUT_VAL,

        -- Остатки на последний день отчетного периода (все)
        SUM((SELECT balance_out_rub FROM "DM".DM_ACCOUNT_BALANCE_F WHERE on_date = v_ToDate
            AND account_rk = balance_rub.ACCOUNT_RK)) AS BALANCE_OUT_TOTAL

    FROM 
       "DM".DM_ACCOUNT_BALANCE_F balance_rub
    LEFT JOIN 
       "DM".DM_ACCOUNT_TURNOVER_F turnover ON balance_rub.account_rk = turnover.account_rk
    JOIN 
       "DS"."MD_ACCOUNT_D" ON balance_rub.account_rk = "DS"."MD_ACCOUNT_D"."ACCOUNT_RK"
    JOIN 
       "DS"."MD_LEDGER_ACCOUNT_S" ON CAST(SUBSTRING(CAST("DS"."MD_ACCOUNT_D"."ACCOUNT_NUMBER" AS VARCHAR), 1, 5) AS VARCHAR) = CAST("DS"."MD_LEDGER_ACCOUNT_S"."LEDGER_ACCOUNT" AS VARCHAR)
    WHERE 
       (balance_rub.on_date = v_FromDate OR balance_rub.on_date = v_ToDate)
    GROUP BY 
       "DS"."MD_LEDGER_ACCOUNT_S"."CHAPTER", SUBSTRING("DS"."MD_ACCOUNT_D"."ACCOUNT_NUMBER", 1, 5), "DS"."MD_ACCOUNT_D"."CHAR_TYPE";

    -- Логирование окончания работы
   UPDATE "LOGS".turnover_log
   SET end_message = 'Завершение заполнения 101 формы за ' || i_OnDate::TEXT,
       end_time = now()
   WHERE log_id = v_log_id;

EXCEPTION WHEN OTHERS THEN
   -- Логирование ошибок
   UPDATE "LOGS".turnover_log
   SET error_message = SQLERRM
   WHERE log_id = v_log_id;
   
   RAISE;
END;
$$
;


DO $$
DECLARE
    report_date DATE := '2018-02-01'; 
BEGIN
    -- Вызов процедуры для расчета витрины DM_F101_ROUND_F
    CALL "DM".fill_f101_round_f(report_date);
    
    RAISE NOTICE 'Расчет витрины DM_F101_ROUND_F завершен за %', report_date;
END $$;


		