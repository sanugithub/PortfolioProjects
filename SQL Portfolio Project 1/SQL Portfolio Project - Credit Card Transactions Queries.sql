/*
Credit Card Transactions Queries

Skills used: Joins, CTE's, Date Functions, Windows Functions, Aggregate Functions, Use cases, Converting Data Types(cast())

*/

--- query to print top 5 cities with highest spends and their percentage contribution of total credit card spends
with c_s as (
select city,sum(amount) as spends
from credit_card_transactions
group by city),
c_ts as (
select *,
sum(spends) over(order by spends desc rows between unbounded preceding and unbounded following) as total_spends
from c_s
),
p_cs as (select *
,cast(spends*100.0/total_spends as decimal(5,2)) as percentage_contribution
from c_ts),
rk_s as (select *,
rank() over(order by spends desc) as rnk
from p_cs)
select city,spends,total_spends,percentage_contribution from rk_s where rnk<=5

--- query to print highest spend month and amount spent in that month for each card type
with cte as (
select card_type,DATEPART(year,transaction_date) as yo,DATENAME(month,transaction_date) as mo
, sum(amount) as monthly_expense
from credit_card_transactions
group by card_type,DATEPART(year,transaction_date),DATENAME(month,transaction_date))
select * from ( 
select *
, rank() over(partition by card_type order by monthly_expense desc) as rn
from cte) A
where rn=1;

-- query to print the transaction details(all columns from the table) for each card type when it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)
with rts as (select *,
sum(amount) over(partition by card_type order by transaction_date,transaction_id rows between unbounded preceding and current row) as running_total_spends
from credit_card_transactions),
fcs as (select *
from rts
where running_total_spends>=1000000)
select * 
from(select *,
rank() over(partition by card_type order by running_total_spends) as rnk
from fcs) A
where rnk=1

-- query to find city which had lowest percentage spend for gold card type
select top 1 city,sum(amount) as total_spend
, sum(case when card_type='Gold' then amount else 0 end) as gold_spend
,sum(case when card_type='Gold' then amount else 0 end)*1.0/sum(amount)*100 as gold_contribution
from credit_card_transactions
group by city
having sum(case when card_type='Gold' then amount else 0 end) > 0
order by gold_contribution

-- query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example: Delhi , bills, Fuel)
with cte as (select city,exp_type,sum(amount) as total_amount
from credit_card_transactions
group by city,exp_type),
cte2 as (select *,
rank() over(partition by city order by total_amount desc) as highest_rnk,
rank() over(partition by city order by total_amount) as lowest_rnk
from cte)
select h_cte.city,h_cte.exp_type as highest_expense_type,l_cte.exp_type as lowest_expense_type
from cte2 as h_cte
inner join cte2 as l_cte on h_cte.city=l_cte.city and h_cte.highest_rnk=l_cte.lowest_rnk
where h_cte.highest_rnk=1 and l_cte.lowest_rnk=1

-- query to find percentage contribution of spends by females for each expense type
with cte as (select exp_type,sum(amount) as total_spend
, sum(case when gender='F' then amount else 0 end) as female_spend
from credit_card_transactions
group by exp_type)
select *,female_spend*100.0/total_spend as female_contribution 
from cte order by female_contribution

-- card and expense type combination with highest month over month growth in Jan-2014
with cte as (select *,format(transaction_date,'yyyyMM') as yr_mth
from credit_card_transactions
where format(transaction_date,'yyyyMM') in (201401,201312)),
cte2 as (select card_type,exp_type,yr_mth,sum(amount) as spends
from cte
group by card_type,exp_type,yr_mth),
cte3 as (select *,
lag(spends,1) over(partition by card_type,exp_type order by yr_mth) as prev_spend
from cte2),
cte4 as (select *,spends-prev_spend as mom_growth
from cte3
where yr_mth=201401)
select * from 
(select card_type,exp_type,yr_mth,spends,prev_spend,mom_growth,
rank() over(order by mom_growth desc) as rnk
from cte4) A
where rnk=1

--during weekends finding city which has highest total spend to total no of transcations ratio
with cte as (select *,datename(weekday,transaction_date) as week_day
from credit_card_transactions
where datename(weekday,transaction_date) in ('Saturday','Sunday')),
cte2 as (select city,sum(amount) as total_spend,count(transaction_id) as total_transaction_count 
from cte
group by city),
cte3 as (select *,
total_spend/total_transaction_count as ratio
from cte2)
select * from 
(select *,
rank() over(order by ratio desc) as rnk
from cte3) A
where rnk=1

-- city which took least number of days to reach its 500th transaction after the first transaction in that city
with cte as (select city,first_transaction_date,last_transaction_date,t_rnk from 
(select *,
first_value(transaction_date) over(partition by city order by transaction_date asc) as first_transaction_date,
last_value(transaction_date) over(partition by city order by transaction_date asc) as last_transaction_date,
row_number() over(partition by city order by transaction_date asc) as t_rnk
from credit_card_transactions) A
where t_rnk=500),
cte2 as (select *,datediff(day,first_transaction_date,last_transaction_date) as no_of_days 
from cte)
select * from 
(select *,
rank() over(order by no_of_days asc) as l_rnk
from cte2) B
where l_rnk=1