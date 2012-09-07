SET mapred.reduce.tasks=30;
drop table if exists lineitem;
drop table if exists orders;
drop table if exists denorm;
drop table if exists part;
drop table if exists supplier;
drop table if exists nation;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists denorm_cached;
drop table if exists region;

create external table region (R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.29.172.95/region';
Create external table lineitem (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT STRING, L_SHIPMODE STRING, L_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.29.172.95/lineitem';
create external table orders (O_ORDERKEY INT, O_CUSTKEY INT, O_ORDERSTATUS STRING, O_TOTALPRICE DOUBLE, O_ORDERDATE STRING, O_ORDERPRIORITY STRING, O_CLERK STRING, O_SHIPPRIORITY INT, O_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.29.172.95/orders';
create external table part (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.29.172.95/part';
create external table supplier (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.29.172.95/supplier';
create external table nation (N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.29.172.95/nation';
create external table partsupp (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST DOUBLE, PS_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION'/10.29.172.95/partsupp';
create external table customer (C_CUSTKEY INT, C_NAME STRING, C_ADDRESS STRING, C_NATIONKEY INT, C_PHONE STRING, C_ACCTBAL DOUBLE, C_MKTSEGMENT STRING, C_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.29.172.95/customer';


create table denorm as
select r_regionkey, l_linenumber, r_name, n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_returnflag, l_shipdate, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_tax, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority, o_comment, c_nationkey, c_custkey, c_name, c_mktsegment 
  from 
  (select * from 
    (select * from
      (select * from
	(select * from
	  (select * from 
	    (select r_regionkey, r_name, n_nationkey, n_name from region r
	      join nation n on n.n_regionkey = r.r_regionkey) nr
	    join supplier s on s.s_nationkey = nr.n_nationkey) nrs
	  join partsupp ps on nrs.s_suppkey = ps.ps_suppkey) nrsps
	join part p on p.p_partkey = nrsps.ps_partkey) nrspsp
      join lineitem l on l.l_partkey = nrspsp.ps_partkey and
			 l.l_suppkey = nrspsp.ps_suppkey) nrspspl
    join orders o on o.o_orderkey = nrspspl.l_orderkey) nrspsplo
  join customer c on c.c_custkey = nrspsplo.o_custkey;

set hive.map.aggr=false;
create table denorm_cached as
select r_regionkey, l_linenumber, r_name, n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_returnflag, l_shipdate, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_tax, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority, o_comment, c_nationkey, c_custkey, c_name, c_mktsegment
from denorm
group by r_regionkey, l_linenumber, r_name, n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_returnflag, l_shipdate, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_tax, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority, o_comment, c_nationkey, c_custkey, c_name, c_mktsegment;
set hive.map.aggr=true;
