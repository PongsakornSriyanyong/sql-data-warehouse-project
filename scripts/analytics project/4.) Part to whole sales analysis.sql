--4)Part to whole analysis
/*
	([Measure]/Total[Measure]) * 100 by [Dimension]
	(sales/Total Sales) * 100 by Category
	(Quantity/ Total Qauntity) * 100 By country
*/
with category_sales as(
select 
category,
sum(sales_amount) total_sales 
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category)
select
category,
total_sales,
sum(total_sales) over() overall_sales,
concat(round((cast (total_sales as float) / sum(total_sales) over())*100,2), '%') as percentage_of_total
from category_sales
order by total_sales desc
