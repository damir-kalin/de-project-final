insert into STV2024031233__DWH.l_transaction(hk_l_transaction, hk_account_from, hk_account_to, hk_currency, hk_operation, load_dt, load_src)
select 
distinct
hash(t.operation_id, t.currency_code, t.account_number_from, t.account_number_to) as hk_l_transaction,
ha.hk_account as hk_account_from,
ht.hk_account as hk_account_to,
hc.hk_currency,
ho.hk_operation,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from STV2024031233__STAGING.transactions t 
	left join STV2024031233__DWH.h_account ha on t.account_number_from = ha.account_number 
	left join STV2024031233__DWH.h_account ht on t.account_number_to = ht.account_number
	left join STV2024031233__DWH.h_currency hc on t.currency_code = hc.currency_code
	left join STV2024031233__DWH.h_operation ho on t.operation_id = ho.operation_id
where hash(t.operation_id, t.currency_code, t.account_number_from, t.account_number_to) not in (select hk_l_transaction from STV2024031233__DWH.l_transaction)
	and t.transaction_dt ::date = '{{ ds }}'::date - interval '1 day';