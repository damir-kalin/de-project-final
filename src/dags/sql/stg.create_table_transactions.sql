drop table if exists STV2024031233__STAGING.transactions;
create table if not exists STV2024031233__STAGING.transactions(
operation_id uuid,
account_number_from int,
account_number_to int,
currency_code int,
country varchar(50),
status varchar(50),
transaction_type varchar(50),
amount int,
transaction_dt timestamp,
constraint pk_transactions primary key(operation_id)
)
order by transaction_dt
SEGMENTED BY hash(operation_id) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);