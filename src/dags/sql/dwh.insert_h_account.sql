insert into STV2024031233__DWH.h_account (hk_account, account_number, load_dt, load_src)
select 
distinct
hash(tr.account_number) as hk_account,
tr.account_number,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from(
select 
account_number_from as account_number,
transaction_dt
from STV2024031233__STAGING.transactions
union
select 
account_number_to as account_number,
transaction_dt
from STV2024031233__STAGING.transactions) as tr
where tr.transaction_dt::date = '{{ ds }}'::date - interval '1 day'
	and tr.account_number not in (select account_number from STV2024031233__DWH.h_account);