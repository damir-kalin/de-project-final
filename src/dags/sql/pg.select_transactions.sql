select 
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
from transactions t
where transaction_dt::date =  '{}'::date
order by transaction_dt;