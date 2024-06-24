select 
	date_update,
	currency_code,
	currency_code_with,
	currency_with_div
from currencies
where date_update::date = '{}'::date
order by date_update;