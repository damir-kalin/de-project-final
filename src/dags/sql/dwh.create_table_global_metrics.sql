drop table if exists STV2024031233__DWH.global_metrics;
create table if not exists STV2024031233__DWH.global_metrics(
date_update date not null,
currency_from int not null,
amount_total numeric(18,2) not null,
cnt_transactions int not null,
avg_transactions_per_account numeric(18,2) not null,
cnt_accounts_make_transactions int not null,
load_dt timestamp not null,
load_src varchar(50) not null,
constraint pk_global_metrics primary key(date_update, currency_from),
constraint uq_global_metrics_date_update_currency_from unique (date_update, currency_from),
constraint ch_global_metrics_amount_total check (currency_from >= 0),
constraint ch_global_metrics_cnt_transactions check (cnt_transactions >= 0),
constraint ch_global_metrics_avg_transactions_per_account check (avg_transactions_per_account >= 0),
constraint ch_global_metrics_cnt_accounts_make_transactions check (cnt_accounts_make_transactions >= 0)
)
order by load_dt
SEGMENTED BY hash(date_update, currency_from) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 90, 60);