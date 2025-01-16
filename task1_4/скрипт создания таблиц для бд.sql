create table if not exists "DM".DM_F101_ROUND_F_v2(
	FROM_DATE date,
TO_DATE date,
CHAPTER char(1),
LEDGER_ACCOUNT char(5),
CHARACTERISTIC char(1),
BALANCE_IN_RUB numeric(23,8),
R_BALANCE_IN_RUB numeric(23,8),
BALANCE_IN_VAL numeric(23,8),
R_BALANCE_IN_VAL numeric(23,8),
BALANCE_IN_TOTAL numeric(23,8),
R_BALANCE_IN_TOTAL numeric(23,8),
TURN_DEB_RUB numeric(23,8),
R_TURN_DEB_RUB numeric(23,8),
TURN_DEB_VAL numeric(23,8),
R_TURN_DEB_VAL numeric(23,8),
TURN_DEB_TOTAL numeric(23,8),
R_TURN_DEB_TOTAL numeric(23,8),
TURN_CRE_RUB numeric(23,8),
R_TURN_CRE_RUB numeric(23,8),
TURN_CRE_VAL numeric(23,8),
R_TURN_CRE_VAL numeric(23,8),
TURN_CRE_TOTAL numeric(23,8),
R_TURN_CRE_TOTAL numeric(23,8),
BALANCE_OUT_RUB numeric(23,8),
R_BALANCE_OUT_RUB numeric(23,8),
BALANCE_OUT_VAL numeric(23,8),
R_BALANCE_OUT_VAL numeric(23,8),
BALANCE_OUT_TOTAL numeric(23,8),
R_BALANCE_OUT_TOTAL numeric(23,8)
);

create table if not exists "LOGS".export_import_log(
	log_id SERIAL PRIMARY KEY,
	start_message TEXT,
	start_time timestamp,
	end_message TEXT,
	end_time TIMESTAMP
);
