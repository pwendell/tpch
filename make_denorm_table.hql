SET mapred.reduce.tasks=6;
drop table if exists denorm;
create table denorm as
select l_linenumber, s_r_name, s_n_name, s_suppkey, s_name, s_address, s_phone, s_acctbal, s_comment, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_returnflag, l_shipdate, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_tax, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority, o_comment, c_nationkey, c_custkey, c_name, c_mktsegment, c_acctbal, c_phone, c_comment, c_address, n_name as c_n_name
  from
  (select * from 
    (select * from 
      (select * from
	(select * from
	  (select * from
	    (select n_name as s_n_name, r_name as s_r_name, s_suppkey,
		    s_name, s_address, s_phone, s_acctbal, s_comment from 
	      (select * from region r
		join nation n on n.n_regionkey = r.r_regionkey) nr
	      join supplier s on s.s_nationkey = nr.n_nationkey) nrs
	    join partsupp ps on nrs.s_suppkey = ps.ps_suppkey) nrsps
	  join part p on p.p_partkey = nrsps.ps_partkey) nrspsp
	join lineitem l on l.l_partkey = nrspsp.ps_partkey and
			   l.l_suppkey = nrspsp.ps_suppkey) nrspspl
      join orders o on o.o_orderkey = nrspspl.l_orderkey) nrspsplo
    join customer c on c.c_custkey = nrspsplo.o_custkey) nrspsploc
  join nation n on n.n_nationkey = nrspsploc.c_nationkey
