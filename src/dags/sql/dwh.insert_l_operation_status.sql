insert into STV2024031233__DWH.l_operation_status(hk_l_operation_status, hk_status, hk_operation, transaction_dt, load_dt, load_src)
select
hash(t.operation_id, t.status, t.transaction_dt) as hk_l_operation_status,
s.hk_status,
o.hk_operation,
t.transaction_dt,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from STV2024031233__STAGING.transactions t 
	left join STV2024031233__DWH.h_operation o on t.operation_id  = o.operation_id 
	left join STV2024031233__DWH.h_status s on t.status = s.status 
where hash(t.operation_id, t.status, t.transaction_dt) not in (select hk_l_operation_status from STV2024031233__DWH.l_operation_status)
	and t.transaction_dt ::date = '{{ ds }}'::date - interval '1 day';