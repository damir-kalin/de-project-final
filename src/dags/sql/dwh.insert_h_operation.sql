insert into STV2024031233__DWH.h_operation(hk_operation, operation_id, load_dt, load_src)
select distinct 
hash(operation_id) as hk_operation,
operation_id,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from STV2024031233__STAGING.transactions
where operation_id not in (select distinct operation_id from STV2024031233__DWH.h_operation)
	and transaction_dt::date = '{{ ds }}'::date - interval '1 day';