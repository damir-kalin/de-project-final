insert into STV2024031233__DWH.global_metrics (date_update, 
											currency_from, 
											amount_total, 
											cnt_transactions, 
											avg_transactions_per_account, 
											cnt_accounts_make_transactions, 
											load_dt, 
											load_src)
with currency_convert as (
	select 
	 hc.hk_currency,
	 lcc.currency_with_div
	 from STV2024031233__DWH.l_convertation_currency lcc 
	 	left join STV2024031233__DWH.h_currency hc on lcc.hk_currency=hc.hk_currency
	 	left join STV2024031233__DWH.h_currency hcc on lcc.hk_currency_with=hcc.hk_currency
	 where hcc.country='usa' and lcc.date_update::date='{{ ds }}'::date - interval '1 day'
),
info_transaction as (
	select 
		los.transaction_dt,
		hc.currency_code,
		ho.operation_id,
		hs.status,
		so.transaction_type,
		lt.hk_account_from,
		lt.hk_account_to,
		case when hc.country = 'usa' then abs(so.amount) else abs(so.amount) * cc.currency_with_div end as amount_convert,
		max(los.transaction_dt) over(partition by ho.operation_id) end_transaction_dt
	from STV2024031233__DWH.l_transaction lt 
		inner join STV2024031233__DWH.h_operation ho on lt.hk_operation = ho.hk_operation 
		inner join STV2024031233__DWH.s_operation so on ho.hk_operation = so.hk_operation 
		inner join STV2024031233__DWH.l_operation_status los on ho.hk_operation = los.hk_operation 
		inner join STV2024031233__DWH.h_status hs on los.hk_status = hs.hk_status 
		inner join STV2024031233__DWH.h_currency hc on lt.hk_currency = hc.hk_currency 
		left join currency_convert cc on cc.hk_currency = hc.hk_currency 
	where los.transaction_dt::date = '{{ ds }}'::date - interval '1 day'
)
select 
	transaction_dt::date,
	currency_code,
	round(sum(case when status='done' then amount_convert else 0 end),2) as amount_total,
	count(distinct operation_id) as cnt_transactions,
	round(count(distinct operation_id) / count(distinct case when right(transaction_type, 8) = 'outgoing' then hk_account_from else hk_account_to end), 2) as avg_transactions_per_account,
	count(distinct case when status='done' then hk_account_from end) as cnt_accounts_make_transactions,
	CURRENT_TIMESTAMP as load_dt,
	'pg_system_resource' as load_src
from info_transaction
where transaction_dt = end_transaction_dt 
group by transaction_dt::date,
	currency_code;