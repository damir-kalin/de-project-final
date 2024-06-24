insert into STV2024031233__DWH.s_operation (hk_operation, transaction_type, amount, load_dt, load_src)
select distinct
o.hk_operation,
t.transaction_type,
t.amount,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from STV2024031233__DWH.h_operation as o
	left join STV2024031233__STAGING.transactions t on o.operation_id=t.operation_id 
where o.hk_operation not in (select hk_operation from STV2024031233__DWH.s_operation)
	and t.transaction_dt ::date = '{{ ds }}'::date - interval '1 day';