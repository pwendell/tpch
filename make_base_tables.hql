drop table if exists lineitem;
drop table if exists orders;
drop table if exists denorm;
drop table if exists part;
drop table if exists supplier;
drop table if exists nation;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists region;

create external table region (R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.28.159.84/region';
Create external table lineitem (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT STRING, L_SHIPMODE STRING, L_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.28.159.84/lineitem';
create external table orders (O_ORDERKEY INT, O_CUSTKEY INT, O_ORDERSTATUS STRING, O_TOTALPRICE DOUBLE, O_ORDERDATE STRING, O_ORDERPRIORITY STRING, O_CLERK STRING, O_SHIPPRIORITY INT, O_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.28.159.84/orders';
create external table part (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.28.159.84/part';
create external table supplier (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.28.159.84/supplier';
create external table nation (N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.28.159.84/nation';
create external table partsupp (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST DOUBLE, PS_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION'/10.28.159.84/partsupp';
create external table customer (C_CUSTKEY INT, C_NAME STRING, C_ADDRESS STRING, C_NATIONKEY INT, C_PHONE STRING, C_ACCTBAL DOUBLE, C_MKTSEGMENT STRING, C_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/10.28.159.84/customer';
