drop table if exists STV2024031233__DWH.l_operation_status cascade;
create table if not exists STV2024031233__DWH.l_operation_status(
	hk_l_operation_status bigint not null,
	hk_status bigint not null,
	hk_operation bigint not null,
	transaction_dt timestamp not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_l_operation_status primary key(hk_l_operation_status),
	constraint uq_hk_l_operation_status unique(hk_status, hk_operation, transaction_dt),
	constraint fk_l_operation_status_hk_status foreign key(hk_status) references STV2024031233__DWH.h_status(hk_status),
	constraint fk_l_operation_status_hk_operation foreign key(hk_operation) references STV2024031233__DWH.h_operation(hk_operation)
)
order by load_dt
SEGMENTED BY hk_l_operation_status all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);