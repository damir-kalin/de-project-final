drop table if exists STV2024031233__DWH.l_transaction cascade;
create table if not exists STV2024031233__DWH.l_transaction(
	hk_l_transaction bigint not null,
	hk_account_from bigint not null,
	hk_account_to bigint not null,
	hk_currency bigint not null,
	hk_operation bigint not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_l_transaction_status primary key(hk_l_transaction),
	constraint uq_hk_l_transaction unique(hk_account_from, hk_account_to, hk_currency, hk_operation),
	constraint fk_l_transaction_hk_operation foreign key(hk_operation) references STV2024031233__DWH.h_operation(hk_operation),
	constraint fk_l_transaction_hk_account_from foreign key(hk_account_from) references STV2024031233__DWH.h_account(hk_account),
	constraint fk_l_transaction_hk_account_to foreign key(hk_account_to) references STV2024031233__DWH.h_account(hk_account),
	constraint fk_l_transaction_hk_currency foreign key(hk_currency) references STV2024031233__DWH.h_currency(hk_currency)
)
order by load_dt
SEGMENTED BY hk_l_transaction all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);