insert into STV2024031233__DWH.h_status (hk_status, status, load_dt, load_src)
select distinct 
hash(status) as hk_status,
status,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from STV2024031233__STAGING.transactions
where status not in (select status from STV2024031233__DWH.h_status)
	and transaction_dt::date = '{{ ds }}'::date - interval '1 day'; 