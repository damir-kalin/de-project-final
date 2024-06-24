drop table if exists STV2024031233__DWH.s_operation cascade;
create table if not exists STV2024031233__DWH.s_operation(
	hk_operation bigint not null,
	transaction_type varchar(50) not null,
	amount int not null,
	load_dt datetime not null,
	load_src varchar(50) not null,
	constraint pk_s_operation primary key(hk_operation),
	constraint fk_h_operation_s_opertion foreign key (hk_operation) references STV2024031233__DWH.h_operation(hk_operation)
)
order by load_dt
SEGMENTED BY hk_operation all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);