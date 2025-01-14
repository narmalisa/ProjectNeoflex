CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id INT;
BEGIN
    -- Логирование начала работы
    INSERT INTO "LOGS".turnover_log (start_message, start_time)
    VALUES ('Начало расчета витрины оборотов за ' || i_OnDate::TEXT, now())
    RETURNING log_id INTO v_log_id;  -- Получаем идентификатор новой записи
	
    -- Удаление предыдущих записей за указанную дату
    DELETE FROM "DM".DM_ACCOUNT_TURNOVER_F WHERE ON_DATE = i_OnDate;

    -- Заполнение витрины оборотов
    INSERT INTO "DM".DM_ACCOUNT_TURNOVER_F (ON_DATE, ACCOUNT_RK, CREDIT_AMOUNT, CREDIT_AMOUNT_RUB, DEBET_AMOUNT, DEBET_AMOUNT_RUB)
    SELECT 
        i_OnDate,
        ft."CREDIT_ACCOUNT_RK" AS ACCOUNT_RK,
        SUM(ft."CREDIT_AMOUNT") AS CREDIT_AMOUNT,
        SUM(ft."CREDIT_AMOUNT") * COALESCE(MIN(er."REDUCED_COURCE"), 1) AS CREDIT_AMOUNT_RUB,
        SUM(ft."DEBET_AMOUNT") AS DEBET_AMOUNT,
        SUM(ft."DEBET_AMOUNT") * COALESCE(MIN(er."REDUCED_COURCE"), 1) AS DEBET_AMOUNT_RUB
    FROM 
        "DS"."FT_POSTING_F" ft
    LEFT JOIN 
        "DS"."MD_EXCHANGE_RATE_D" er ON er."DATA_ACTUAL_DATE" = i_OnDate
    WHERE 
        ft."OPER_DATE" = i_OnDate
    GROUP BY 
        ft."CREDIT_ACCOUNT_RK"
		
	UNION ALL

	SELECT 
        i_OnDate,
        ft."DEBET_ACCOUNT_RK" AS ACCOUNT_RK,
        SUM(ft."CREDIT_AMOUNT") AS CREDIT_AMOUNT,
        SUM(ft."CREDIT_AMOUNT") * COALESCE(MIN(er."REDUCED_COURCE"), 1) AS CREDIT_AMOUNT_RUB,
        SUM(ft."DEBET_AMOUNT") AS DEBET_AMOUNT,
        SUM(ft."DEBET_AMOUNT") * COALESCE(MIN(er."REDUCED_COURCE"), 1) AS DEBET_AMOUNT_RUB
    FROM 
        "DS"."FT_POSTING_F" ft
    LEFT JOIN 
        "DS"."MD_EXCHANGE_RATE_D" er ON er."DATA_ACTUAL_DATE" = i_OnDate
    WHERE 
        ft."OPER_DATE" = i_OnDate
    GROUP BY 
        ft."DEBET_ACCOUNT_RK";

   -- Логирование окончания работы
   UPDATE "LOGS".turnover_log
   SET end_message = 'Завершение расчета витрины оборотов за ' || i_OnDate::TEXT,
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
    current_date_ DATE := '2018-01-01'; -- Начальная дата
BEGIN
    WHILE current_date_ <= '2018-01-31' LOOP
        -- Вызов процедуры для расчета витрины оборотов за текущую дату
        CALL ds.fill_account_turnover_f(current_date_);
        
        -- Переход к следующему дню
        current_date_ := current_date_ + INTERVAL '1 day';
    END LOOP;
END $$;

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f() LANGUAGE plpgsql AS $$ 
DECLARE
     v_log_id INT;
BEGIN
    -- Удаление старых записей
    DELETE FROM "DM".DM_ACCOUNT_BALANCE_F WHERE ON_DATE = '2017-12-31';

    -- Логирование начала работы
    INSERT INTO "LOGS".turnover_log (start_message, start_time)
    VALUES ('Начало расчета витрины остатков за 31-12-2017 ', now())
    RETURNING log_id INTO v_log_id;  -- Получаем идентификатор новой записи


    -- Расчет витрины остатков
    INSERT INTO "DM".DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
    SELECT 
        '2017-12-31' AS on_date,
        fb."ACCOUNT_RK",
        fb."BALANCE_OUT",
        fb."BALANCE_OUT" * COALESCE(er."REDUCED_COURCE", 1) AS balance_out_rub
    FROM 
        "DS"."FT_BALANCE_F" fb
    LEFT JOIN 
        "DS"."MD_EXCHANGE_RATE_D" er ON er."DATA_ACTUAL_DATE" = '2017-12-31';

   -- Логирование окончания работы
   UPDATE "LOGS".turnover_log
   SET end_message = 'Завершение расчета витрины остатков за 31-12-2017',
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

call ds.fill_account_balance_f() 

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_id INT;
BEGIN
    -- Логирование начала работы
    INSERT INTO "LOGS".turnover_log (start_message, start_time)
    VALUES ('Начало расчета витрины остатков за ' || i_OnDate::TEXT, now())
    RETURNING log_id INTO v_log_id;  -- Получаем идентификатор новой записи

    -- Удаление предыдущих записей за указанную дату
    DELETE FROM "DM".DM_ACCOUNT_BALANCE_F WHERE ON_DATE = i_OnDate;

    -- Заполнение витрины остатков
    INSERT INTO "DM".DM_ACCOUNT_BALANCE_F (ON_DATE, ACCOUNT_RK, BALANCE_OUT, BALANCE_OUT_RUB)
    SELECT 
        i_OnDate,
        a."ACCOUNT_RK",
        CASE 
            WHEN a."CHAR_TYPE" = 'А' THEN 
                COALESCE(prev.BALANCE_OUT, 0) + COALESCE(turnover.DEBET_AMOUNT, 0) - COALESCE(turnover.CREDIT_AMOUNT, 0)
            WHEN a."CHAR_TYPE" = 'П' THEN 
                COALESCE(prev.BALANCE_OUT, 0) - COALESCE(turnover.DEBET_AMOUNT, 0) + COALESCE(turnover.CREDIT_AMOUNT, 0)
        END AS BALANCE_OUT,
        CASE 
            WHEN a."CHAR_TYPE" = 'А' THEN 
                COALESCE(prev.BALANCE_OUT_RUB, 0) + COALESCE(turnover.DEBET_AMOUNT_RUB, 0) - COALESCE(turnover.CREDIT_AMOUNT_RUB, 0)
            WHEN a."CHAR_TYPE" = 'П' THEN 
                COALESCE(prev.BALANCE_OUT_RUB, 0) - COALESCE(turnover.DEBET_AMOUNT_RUB, 0) + COALESCE(turnover.CREDIT_AMOUNT_RUB, 0)
        END AS BALANCE_OUT_RUB
    FROM 
        "DS"."MD_ACCOUNT_D" a
    LEFT JOIN 
        "DM".DM_ACCOUNT_TURNOVER_F turnover ON a."ACCOUNT_RK" = turnover.ACCOUNT_RK AND turnover.ON_DATE = i_OnDate
    LEFT JOIN 
        "DM".DM_ACCOUNT_BALANCE_F prev ON a."ACCOUNT_RK" = prev.ACCOUNT_RK AND prev.ON_DATE = (i_OnDate - INTERVAL '1 DAY')
    WHERE 
        a."DATA_ACTUAL_DATE" <= i_OnDate AND a."DATA_ACTUAL_END_DATE" >= i_OnDate;

   -- Логирование окончания работы
   UPDATE "LOGS".turnover_log
   SET end_message = 'Завершение расчета витрины остатков за ' || i_OnDate::TEXT,
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
    start_date DATE := '2018-01-01';
    end_date DATE := '2018-01-31';
    current_date_ DATE;
BEGIN
    current_date_ := start_date;

    WHILE current_date_ <= end_date LOOP
        -- Вызов процедуры для расчета витрины остатков за текущую дату
        CALL ds.fill_account_balance_f(current_date_);
        
        -- Переход к следующему дню
        current_date_ := current_date_ + INTERVAL '1 DAY';
    END LOOP;

    RAISE NOTICE 'Расчет витрины остатков завершен за период с % по %', start_date, end_date;
END $$;










