/*Cust_info quality check*//////////////////////////////////////////////////////////////
--Quality Check

-- Check Primary key (not null, Duplicate) 
select cst_id,count(*) from silver.crm_cust_info
group by cst_id having count(*) > 1 or cst_id IS NULL

--Check Unwanted Spaces
Select cst_firstname
from silver.crm_cust_info
Where cst_firstname != TRIM(cst_firstname) --Trim = ?????????????

Select cst_lastname
from silver.crm_cust_info
Where cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
Select Distinct cst_gndr
from silver.crm_cust_info

-- Material
Select cst_material_status
from silver.crm_cust_info

select * from silver.crm_cust_info

/*prd_info quality check*////////////////////////////////////////////////////////
--Check Quality
-- check dupplicate prd_id
select prd_id,count(*) from silver.crm_prd_info
group by prd_id having count(*) > 1 or prd_id is null

-- check trim prd_nm
select prd_nm from silver.crm_prd_info where prd_nm != Trim(prd_nm)

-- check null and negative price
select prd_cost from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Data standatdization & Consistency
select distinct prd_line
from silver.crm_prd_info

-- Check Date order
select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt

/*sales_details_quality_check*/////////////////////////////////////////////////////////
-- check for Invalid Dates (order dt)
Select
nullif(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

-- check for Invalid Dates (sls_due_date)
Select
nullif(sls_order_dt,0) sls_due_dt
FROM silver.crm_sales_details
where sls_due_dt <= 0 
or len(sls_due_dt) != 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101

-- check for invalid date orders
select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt >sls_ship_dt

-- check for sales = quantity*price (negative, null are not allowed)
select sls_sales,sls_quantity,sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price

select * from silver.crm_sales_details

