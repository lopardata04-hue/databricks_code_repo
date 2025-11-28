--drop table rawds.trans_online;
--drop table rawds.consumer;
--drop table rawds.trans_pos;
--drop table rawds.trans_mobile_channel;
--drop table curatedds.consumer_full_load;
--drop table curatedds.trans_online_part;
--drop table curatedds.trans_pos_part_cluster;
--drop table `curatedds.trans_mobile_autopart_2021`;
--drop table `curatedds.trans_mobile_autopart_2022`;
--drop table `curatedds.trans_mobile_autopart_2023`;

create schema if not exists rawds options(location='us');
create table if not exists rawds.audit_meta(proc_name string,exec_ts timestamp,description string);
select current_timestamp,"Load started";
insert into rawds.audit_meta select 'rawload',current_timestamp,"1. Store POS - Create and Load CSV data into BQ Managed table with defined schema";
select current_timestamp,"1. Store POS - Create and Load CSV data into BQ Managed table with defined schema";
--Combining (create table statement / truncate table + load data)
-- in hive, we have to create table seperately, truncate table seperately then load data seperately

LOAD DATA OVERWRITE `rawds.trans_pos` (txnno numeric,txndt string,custno int64,amt float64,category string,product string,city string, state string, spendby string)
  FROM FILES (
    format = 'CSV', uris = ['gs://consumer-analytics-bucket/data/store_pos_product_trans.csv'],
    field_delimiter=',');
--equivalent to header=False, structype+schema=strct1 or toDF("colname1","colname2"),sep/delimiter=','

--logging operation
insert into rawds.audit_meta 
select 'rawload',current_timestamp,"2. Online Trans - Create and Load JSON data into BQ Managed table using auto detect schema";
--print statement in python or echo in linux
select current_timestamp,"2. Online Trans - Create and Load JSON data into BQ Managed table using auto detect schema";

LOAD DATA OVERWRITE rawds.trans_online
  FROM FILES (
    format = 'JSON', uris = ['gs://consumer-analytics-bucket/data/online_products_trans.json']);

insert into rawds.audit_meta 
select 'rawload',current_timestamp,"3. Consumer Dimension - Create manually the table and Load CSV data into BQ Managed table, skip the header column in the file";
select current_timestamp,"3. Consumer Dimension - Create manually the table and Load CSV data into BQ Managed table, skip the header column in the file";

create table if not exists rawds.consumer(
custno INT64, 
firstname STRING,
lastname STRING,
age INT64,
profession STRING);

LOAD DATA OVERWRITE rawds.consumer
  FROM FILES (
    format = 'CSV', uris = ['gs://consumer-analytics-bucket/data/custs_header'],
    skip_leading_rows=1,field_delimiter=',');
--equivalent to header=True, structype+schema=strct1,sep/delimiter=','

insert into rawds.audit_meta 
select 'rawload',current_timestamp,"4. Mobile Trans - Create the table using auto detect schema using the header column and Load CSV data into BQ Managed table";

select current_timestamp,"4. Mobile Trans - Create the table using auto detect schema using the header column and Load CSV data into BQ Managed table";

LOAD DATA OVERWRITE `rawds.trans_mobile_channel`
  FROM FILES (
    format = 'CSV', uris = ['gs://consumer-analytics-bucket/data/mobile_trans.csv'],
    field_delimiter=',');

insert into rawds.audit_meta 
select 'rawload',current_timestamp,"5. Load completed Successfully";