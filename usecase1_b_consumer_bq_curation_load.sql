--Declare and set is equivalent to assigning a variable with value in other prog. languages
--eg. in python we create load_ts=datetime.now();
declare loadts timestamp;
set loadts=(select current_timestamp());
--loadts=2025-07-16 08:55:10
begin
begin
--How to achieve Complete Refresh (delete the entire target data and reload with the entire source data)
--option1:drop the existing table and recreate the table with the same structure then later reload the entire customer data (both structure and data deleted)
--option2:Truncate table and insert option can be used to load the table later (if the entire data has to be deleted) (entire data (only) deleted)
--option2:****insert overtwrite into table option is not supported in BQ as like hive
--option3:delete table (only the given date/hour) and insert option can be used later (entire/portion of data (only) deleted)
--interview questions: Difference between drop (create or replace table) (preference2), truncate (preference1) and delete(preference3).
--interview questions: Difference between Hive & Bigquery - (Bigquery doesn't support insert overtwrite and hive doesn't support  create or replace table.
--interview questions: Difference between Hive & Bigquery - (Bigquery support delete and hive doesn't support Delete directly (we need some workaround)
create schema if not exists curatedds options(location='us');

create or replace table curatedds.consumer_full_load(
custno INT64, 
fullname STRING,
age INT64,
yearofbirth INT64,
profession STRING,
loadts timestamp,
loaddt date);
--Interview1: What is the difference between Drop & Create or Truncate or Delete ?
--above query will drop the table if exists and recreate it...
--truncate table curatedds.consumer_full_load;
--delete from curatedds.consumer_full_load where filters....;

--Interview2: 
-- Tell me the migration/modernization challenges you faced between hive and bigquery?
-- Tell me about partition concept in Bigquery, why we needed and when do we go for partitioning?
--It is same like hive, ie. partition will store the data internally into the given partition that is to improve filter performance by avoiding FTS (FULL TABLE SCAN)
-- Tell me in a table how many partition columns can be created and what could be the datatypes?
--Only ONE (Level) partition column and datatype can be either date/integer and not string
-- How many values can be kept in a partition column?
--maximum 4000 unique values for eg. in a date partition we can store upto 11 years date values (11*365)
--& How it is different from Hive?
--Biquery will support only date or integer columns type and not string for partitioning, where as hive supports string and other datatypes also
--Biquery will support only single partitioning columns (no multi level partition), where as hive supports multiple (multi level) partitions columns
--Syntax wise, bigquery can have the column for partition in both create table columns list and partition list also, where as hive only support the column in the partition which should not be defined in the create table columns list.
--hive -> create table tbl1(id int) partitioned by (load_dt date);
--bq -> create table tbl1(id integer,load_dt date) partition by load_dt;

--Partition management in Bigquery is simplified eg. We don't have msck repair/refreshing of the partition by keeping data in hdfs are not needed as like hive (because hive has metadata layer and data layer seperated).

--*** Important - challenge in migrating from hive to BQ
-- Since, insert overtwrite into table option is not supported in BQ as like hive, we can't do insert overwrite partition also.
-- as a workaround, we have to delete the data seperately, then load into the partition table. delete from table where loaddt=loaddt(variable)

--Partitions can be created only on the low cardinal Date & Number columns, Maximum number of partitions can be only 4000

create table if not exists curatedds.trans_online_part
(transsk numeric,customerid int64,productname string,productcategory string,productprice int64,productidint int64,prodidstr string ,loadts timestamp,loaddt date)
partition by loaddt
OPTIONS (require_partition_filter = FALSE);
--require_partition_filter will enforce a mandatory partition filter to be used mandatorily for the user of the table

--Interview3: When do we go for Clustering in BQ? For join and filter performance improvement
-- Maximum cluster by columns can be only 4 in BQ, hive can support any numbers
-- Difference between Hive bucketing and BQ clustering? Both are same technically, but syntax varies..
-- Clustering columns order of defining is very important
--Bigquery supports clustering (sorting and grouping/bucketing of the clustered columns) to improve filter and join performances
--BQ clustering is exactly equivalent to hive bucketing only syntax varies, symantics remains same...
--bq syntax(cluster by col1,col2 upto max col4), hive syntax is (clustered by col1,col2... into 3 buckets sort by col1,col2)

create table  if not exists curatedds.trans_pos_part_cluster
(txnno numeric,txndt date,custno int64,amount float64,category string,product string,city string, state string, spendby string,loadts timestamp,loaddt date)
partition by loaddt
cluster by loaddt,custno,txnno
OPTIONS (description = 'point of sale table with partition and clusters');
end;

--Block1 for loading raw consumer data to the curated consumer table (Full Refresh) - using inline views (some name for the memory)
begin
--Interview4: Difference between delete, truncate and drop+recreate (create or replace table)
--delete -> DML, it help us delete the portion of data applying where clause and in BQ it is mandatory to use where clause (DELETE must have a WHERE clause at [1:1])
--truncate -> DDL, it help us delete the data entire data (without affecting the structure) without applying any filters (faster than delete).
--drop -> DDL, it help us drop the table structure and data also.
--truncate table curatedds.consumer_full_load;
--DATA MUNGING
--clensing - de duplication
--scrubbing - replacing of null with some values
--curation (business logic) - concat 
--Data enrichment - deriving a column from exiting or new column
--inline view (from clause subquery) is good for nominal data size, but not good for holding large volume of data in memory.. if volume is large, go with temp view.
----FROM CLAUSE SUB QUERY OR INLINE VIEW eg. select cols from (select cols from tbl) as view;
--We have an alternative for inline view by using qualify function
--This is a Slowly Changing Dimension Type 1 Load (Since we are not maintaining History)
--truncate table curatedds.consumer_full_load;
/*
#subquery
select * from rawds.consumer where custid = (select max(custid) from rawds.consumer)
#correlated subquery (outer query depend on inner query & vice versa)
For every row of execution of a parent query the child query will be executed and based on the child query 
result the parent query will run..
select * from rawds.consumer parent where age = (select max(age) from rawds.consumer child where child.custid=parent.custid) and custid=4000011
*/
INSERT INTO curatedds.consumer_full_load
SELECT 
    custid,
    CONCAT(firstname, ' ', lastname) AS fullname,
    age,
    EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE, INTERVAL age YEAR)) AS yearofbirth,
    COALESCE(profession, 'not provided') AS profession,
    loadts,
    DATE(loadts) AS loaddt
FROM (
    SELECT 
        custid, 
        firstname, 
        lastname, 
        age, 
        profession
    FROM (
        SELECT 
            custid, 
            firstname, 
            lastname, 
            age, 
            profession,
            ROW_NUMBER() OVER (PARTITION BY custid ORDER BY age DESC) AS rnk
        FROM rawds.consumer
		--qualify rnk=1 #will help us avoid inline view
    ) AS inline_view
    WHERE rnk = 1
) AS dedup;

end;

begin
--Interview5:
--In BigQuery, a temporary table is a session-scoped table that stores data in the underlying Colossus storage, 
-- not in memory like inline views.
--Temporary tables persist only for the duration of the session (such as a BEGIN ... END block or script).
--Use temporary tables when working with large intermediate results that may not fit well in memory if handled with inline views.
--They improve performance, reusability, and make complex queries easier to structure by storing intermediate results that can be reused multiple times within the same script or session or using the trick of getting the session table rom the job information, we can use it outside of the session also for a day.
--Additional Features of temp table for interview purpose.
/*
Auto-Cleanup - within the session or actually 1 day expiration
Faster for Intermediates - since it uses Colossus
Cost-Efficient - no long term storage (only 1 day max)
Session-Scoped - no conflict with other sessions
Helps in cases where Common Table Expressions (CTEs) or Inline view might not perform well
*/

--Curation Logic - 
--Converting from Semistructured to structured using unnest function (equivalent to explode in hive)
--generating surrogate key (unique identifier)
--Splitting of columns by extracting the string and number portion from the product column 
--select regexp_replace('4B2A', '[^0-9]','')
--below stardardization will convert blankspace/nulls/number only to blankspace then to 'na', allow only the string portition to the target otherwise
--case when coalesce(trim(regexp_replace(lower(prod_exploded.productid),'[^a-z]','')),'')='' then 'na' 
--adding some date and timestamp columns for load data.

--generate_uuid will generate unique id alphanumeric value, convert to number (hashing) using farm_fingerprint, make it positive value using abs function

/* How the un nesting is happening / semistruct to structure conversion using unnest equivalent to explode in hive
{"orderId":"1","storeLocation":"Newyork","customerId" : "4000011",
"products":[{"productId":"1234","productCategory":"Laptop","productName" : "HP Pavilion","productPrice": "540"},
{"productId":"421","productCategory":"Mobile","productName" : "Samsung S22","productPrice": "240"}]}
Nested (semi structured):
orderId,storeLocation,customerId,products
								 productId,productCategory,productName,productPrice
1      ,Newyork,      4000011   ,1234,     Laptop,         HP Pavilion,540
                                 421,      Mobile,         Samsung S22,240
Un Nested (structured):
orderId,storeLocation,customerId,products.productId,products.productCategory,products.productName,products.productPrice
1      ,Newyork,      4000011   ,1234,     Laptop,         HP Pavilion,540
1      ,Newyork,      4000011   ,421,      Mobile,         Samsung S22,240
*/
CREATE OR REPLACE TEMP TABLE online_trans_view AS
SELECT 
    ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS transk,
    customerid,
    prod_exploded.productname,
    prod_exploded.productcategory,
    prod_exploded.productprice,
    
    -- Extract numeric part of product ID
    CAST(REGEXP_REPLACE(prod_exploded.productid, '[^0-9]', '') AS INT64) AS prodidint,
    -- Extract alphabetic part or default to 'na'
    CASE 
        WHEN COALESCE(TRIM(REGEXP_REPLACE(LOWER(prod_exploded.productid), '[^a-z]', '')), '') = '' 
        THEN 'na'
        ELSE REGEXP_REPLACE(LOWER(prod_exploded.productid), '[^a-z]', '')
    END AS prodidstr,
    loadts,
    DATE(loadts) AS loaddt

FROM `rawds.trans_online` p,
UNNEST(p.products) AS prod_exploded;


----Nested
--4000015,[{Table,Furniture,440,34P},{Chair,Furniture,440,42A}]
--Unnested
--4000015,Table,Furniture,440,34P
--4000015,Chair,Furniture,440,42A

--visagan - I saw this delete statement based on load date with in our project in the begining of the each query file  and they said its for job restartability/rerun of the jobs
delete from curatedds.trans_online_part where loaddt=date(loadts);

--as like hive, we don't need parition(), we can't use insert overwrite - 
--insert overwrite table curatedds.trans_online_part partition(loaddt) select * from online_trans_view;
--in BQ we have to delete the partition explicitly and load the data, if any data is going to be repeated.
--Loading of partition table in BQ, 

insert into curatedds.trans_online_part select * from online_trans_view;

--to understand reusability feature of the temp table.
--how to create a table structure from another table without loading data.
create table if not exists curatedds.trans_online_part_furniture as select * from online_trans_view where 1=2;
truncate table curatedds.trans_online_part_furniture;
insert into curatedds.trans_online_part_furniture select * from online_trans_view where productcategory='Furniture';
end;

begin
--CURATION logic
--date format conversion and casting of string to date
--data conversion of product to na if null is there
--converted columns/some of the selected columns in an order, are not selected again by using except function
insert into curatedds.trans_pos_part_cluster 
SELECT txnno,parse_date('%m-%d-%Y',txndt) txndt,custno,amt,coalesce(product,'NA'),* except(txnno,txndt,custno,amt,product),loadts,date(loadts) as loaddt 
FROM `rawds.trans_pos`;
end;

begin
--curation logic
--In the below usecase, storing the data into 3 year tables, because ...
--To improve the performance of picking only data from the given year table
--To reduce the cost of storing entired data in more frequent data access layer (active storage), rather old year may be accessed once in a quarter or half yearly or yearly which data is stored in (long term storage) of BQ saves the cost.
--Drawback is - we need to combine all these tables using union all or union distinct to get all table data in one shot, but BQ 
--provides a very good feature of wild card search for the table names... eg. select * from `dataset.tablename*`
--merging of columns of date and time to timestamp column
--merging of long and lat to geopoint data type to plot in the google map to understand where was my customer when the trans was happening

--Usecases: autopartition (without defining our own partition, BQ itself will create and maintain the partition of the internal clock load date) - simply we can still create date partitions if we don't have date data in our dataset.
--geography datatype - is used to hold the long,lat gps (geo) coordinates of the location.
--wildcard table queries
--table segregation for all the above reasons mentioned.
--for current year 2023 date alone, we are making the future date as current date using INLINE VIEW/FROM CLAUSE SUBQUERY, to correct the data issue from the source.

CREATE TABLE if not exists curatedds.trans_mobile_autopart_2021
(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
PARTITION BY
  _PARTITIONDATE;

insert into `curatedds.trans_mobile_autopart_2021` (txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)
select txnno,cast(dt as date) dt,timestamp(concat(dt,' ',hour)) as ts,ST_GEOGPOINT(long,lat) geo_coordinate,net,provider,activity,postal_code,town_name,loadts,date(loadts) as loaddt
from `rawds.trans_mobile_channel`
where extract(year from cast(dt as date))=2021;


CREATE TABLE  if not exists curatedds.trans_mobile_autopart_2022
(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
PARTITION BY
  _PARTITIONDATE;

insert into curatedds.trans_mobile_autopart_2022 (txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)
select txnno,cast(dt as date) dt,timestamp(concat(dt,' ',hour)) as ts,ST_GEOGPOINT(long,lat) geo_coordinate,net,provider,activity,postal_code,town_name,loadts,date(loadts) as loaddt
from `rawds.trans_mobile_channel`
where extract(year from cast(dt as date))=2022;

CREATE TABLE  if not exists curatedds.trans_mobile_autopart_2023
(txnno numeric,dt date,ts timestamp,geo_coordinate geography,net string,provider string,activity string,postal_code int,town_name string,loadts timestamp,loaddt date)
PARTITION BY
  _PARTITIONDATE;

insert into curatedds.trans_mobile_autopart_2023 (txnno,dt,ts,geo_coordinate,net,provider,activity,postal_code,town_name,loadts,loaddt)  
select txnno,cast(dt as date) dt,timestamp(concat(dt,' ',hour)) as ts,ST_GEOGPOINT(long,lat) geo_coordinate,net,provider,activity,postal_code,town_name,loadts,date(loadts) as loaddt
from (select case when cast(dt as date)>current_date() then current_date() else dt end as dt,* except(dt) from `rawds.trans_mobile_channel`) t
where extract(year from cast(dt as date))=2023;
end;

end;