--2) Cumulative Analysis
--Calculate the total sales per month
--and the running total of sales over time
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
order by datetrunc(month,order_date) 

select
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales --+ stack running sale 1 + total_sales 2 = running sales 2
from(
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t;

select
order_date,
total_sales,
sum(total_sales) over (partition by order_date order by order_date) as running_total_sales --+ stack running sale 1 + total_sales 2 = running sales 2
from(
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t

select
order_date,
total_sales,
sum(total_sales) over (partition by order_date order by order_date) as running_total_sales,--+ stack running sale 1 + total_sales 2 = running sales 2
avg(avg_price) over (order by order_date) as moving_average_price
from(
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
)t
