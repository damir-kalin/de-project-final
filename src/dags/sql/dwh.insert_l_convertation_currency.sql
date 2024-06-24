insert into STV2024031233__DWH.l_convertation_currency(hk_l_convertation_currency, hk_currency, hk_currency_with, date_update, currency_with_div, load_dt, load_src)
select 
hash(c.currency_code, c.currency_code_with, c.date_update),
cu.hk_currency,
cuw.hk_currency as hk_currency_with,
c.date_update,
c.currency_with_div,
CURRENT_TIMESTAMP as load_dt,
'pg_system_resource' as load_src
from STV2024031233__STAGING.сurrencies as c
	left join STV2024031233__DWH.h_currency as cu on c.currency_code = cu.currency_code
	left join STV2024031233__DWH.h_currency as cuw on c.currency_code_with = cuw.currency_code
where hash(c.currency_code, c.currency_code_with, c.date_update) not in (select hk_l_convertation_currency from STV2024031233__DWH.l_convertation_currency)
	and c.date_update ::date = '{{ ds }}'::date - interval '1 day';