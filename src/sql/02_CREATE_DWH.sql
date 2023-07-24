-- Drop table global_metrics
DROP TABLE IF EXISTS STV2023060656__DWH.global_metrics;

-- Create table global_metrics
CREATE TABLE STV2023060656__DWH.global_metrics
(
    id bigint NOT NULL,
	date_update timestamp(0) NOT NULL,
	currency_from int NOT NULL,
	amount_total int NOT NULL,
	cnt_transactions int NOT NULL,
	avg_transactions_per_account numeric(9, 3) NOT NULL,
	cnt_accounts_make_transactions int NOT NULL,
	CONSTRAINT global_metrics_pkey PRIMARY KEY (id)
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);