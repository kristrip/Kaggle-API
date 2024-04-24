SELECT * from kaggle_retail_orders

--find top 10 highest revenue generating products 
SELECT top 10 product_id, sum(sale_price) as revenue
 FROM kaggle_retail_orders
 GROUP BY product_id order by sum(sale_price) desc


--find top 5 highest selling products in each region (QTY)
with cte as
(SELECT region, product_id, sum(quantity) as total_qty
 FROM kaggle_retail_orders
 GROUP BY region, product_id),

 rank_cte as 
(SELECT region, product_id, total_qty, DENSE_RANK() over(Partition by region order by total_qty desc) as DENSE_RANK
 from cte)

 SELECT * from rank_cte where DENSE_RANK <= 10

 --find top 5 highest selling products in each region (sales)
  -- use can also use rank, dense_rank, row_number depending on the situation
 with cte as
(SELECT region, product_id, sum(sale_price) as sales
 FROM kaggle_retail_orders
 GROUP BY region, product_id),

 rank_cte as 
(SELECT region, product_id, sales, DENSE_RANK() over(Partition by region order by sales desc) as DENSE_RANK_sales
 from cte)

 SELECT region, product_id, sales from rank_cte where DENSE_RANK_sales <= 10

 --find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023

 WITH DateRange AS (
  SELECT cast('2022-01-01' as date) AS dateValue
  UNION ALL
  SELECT DATEADD(day, 1, dateValue)
  FROM DateRange
  WHERE dateValue < '2024-01-01'
 ),
 cte as 
 (
    SELECT order_date, sum(sale_price) as sales_date from kaggle_retail_orders GROUP BY order_date
 ),

 full_cte as
 (
    SELECT d.dateValue, c.sales_date
 FROM DateRange d left join cte c on d.dateValue = c.order_date
 
 ),
 part1 as
(SELECT MONTH(dateValue) as '2022', sum(sales_date) as sales_2022
 from full_cte
 where year(dateValue) = 2022
 GROUP BY MONTH(dateValue)),

 part2 as
(SELECT MONTH(dateValue) as '2023', sum(sales_date) as sales_2023
 from full_cte
 where year(dateValue) = 2023
 GROUP BY MONTH(dateValue)),

final_cte as 
(
SELECT part1.[2022] as month, cast(((part2.sales_2023 - part1.sales_2022) *1.0 / part1.sales_2022 * 1.0) * 100 as decimal (7,2)) as perectage_change
 from part1 join part2 on part1.[2022] =part2.[2023]
)
 SELECT [month], CONCAT(perectage_change,'%') as perectage_change
 from final_cte
OPTION(maxrecursion 32767)

 --find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023
with cte as (
select year(order_date) as order_year,month(order_date) as order_month,
sum(sale_price) as sales
from kaggle_retail_orders
group by year(order_date),month(order_date)
-- order by year(order_date),month(order_date)
	)
select order_month
, sum(case when order_year=2022 then sales else 0 end) as sales_2022
, sum(case when order_year=2023 then sales else 0 end) as sales_2023
from cte 
group by order_month
order by order_month

--for each category which month had highest sales 

with cte as 
(
SELECT category, MONTH(order_date) as month, sum(sale_price) as sales
from kaggle_retail_orders
group by category, MONTH(order_date)
)
SELECT category, month, sales from
(select *, ROW_NUMBER() over(partition by category order by sales desc) as rn from cte) a
where a.rn = 1

--for each category which month had highest sales consider yer also 
with cte as 
(
SELECT category, YEAR(order_date) as Year,MONTH(order_date) as month,  sum(sale_price) as sales
from kaggle_retail_orders
group by category,YEAR(order_date), MONTH(order_date)
)
SELECT category,concat(year,' - ' ,month) as Months, sales from
(select *, ROW_NUMBER() over(partition by category, year order by sales desc) as rn from cte) a
where a.rn = 1

--for each category which month had highest sales consider yer also (using format function)
with cte as 
(
SELECT category, FORMAT(order_date, 'yyyy-MMMMMMMMMMM') as Interval , sum(sale_price) as sales
from kaggle_retail_orders
GROUP by category, FORMAT(order_date, 'yyyy-MMMMMMMMMMM') 
)
SELECT category,Interval, sales, rn from
(select *, ROW_NUMBER() over(partition by category order by sales desc) as rn from cte) a
where a.rn = 1
order by category

--which sub category had highest growth by profit in 2023 compare to 2022
with cte as
(
SELECT sub_category, YEAR(order_date) as YEAR , SUM(profit) as profit
from kaggle_retail_orders
GROUP BY sub_category, YEAR(order_date)
),
year_cte as 
(
    SELECT sub_category,
case when [YEAR] = 2022 then profit else 0 end as [2022],
case when [YEAR] = 2023 then profit else 0 end as [2023]
from cte
)
select TOP 1 sub_category, sum([2023]) - sum([2022]) as Year_profit_diff from year_cte
GROUP by sub_category
order by Year_profit_diff DESC