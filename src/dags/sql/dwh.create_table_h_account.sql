create table if not exists STV2024031233__DWH.h_account(
	hk_account bigint not null,
	account_number bigint not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_h_account primary key(hk_account),
	constraint uq_h_account_account_number unique(account_number)
)
order by load_dt
SEGMENTED BY hk_account all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);