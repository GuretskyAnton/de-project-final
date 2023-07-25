-- Drop table transactions
DROP TABLE IF EXISTS STV2023060656__STAGING.transactions;

-- Create table transactions
CREATE TABLE STV2023060656__STAGING.transactions
(
	operation_id varchar(60),
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt timestamp(0)
)
order by transaction_dt, operation_id
SEGMENTED BY hash(transaction_dt::date, operation_id) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);

CREATE PROJECTION STV2023060656__STAGING.transactions /*+createtype(P)*/ 
(
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
)
AS
 	SELECT 	transactions.operation_id,
			transactions.account_number_from,
			transactions.account_number_to,
			transactions.currency_code,
			transactions.country,
			transactions.status,
			transactions.transaction_type,
			transactions.amount,
			transactions.transaction_dt
 	FROM STV2023060656__STAGING.transactions
 	ORDER BY transactions.transaction_dt,
        transactions.operation_id
SEGMENTED BY hash((transactions.transaction_dt)::date, transactions.operation_id) ALL NODES KSAFE 1;


-- Drop table currencies
DROP TABLE IF EXISTS STV2023060656__STAGING.currencies;

-- Create table currencies
CREATE TABLE STV2023060656__STAGING.currencies
(
	date_update timestamp(0),
	currency_code int,
	currency_code_with int,
	currency_with_div numeric(5, 3)
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION STV2023060656__STAGING.currencies /*+createtype(P)*/ 
(
 date_update,
 currency_code,
 currency_code_with,
 currency_with_div
)
AS
 SELECT currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_with_div
 FROM STV2023060656__STAGING.currencies
 ORDER BY currencies.date_update
SEGMENTED BY hash((currencies.date_update)::date) ALL NODES KSAFE 1;