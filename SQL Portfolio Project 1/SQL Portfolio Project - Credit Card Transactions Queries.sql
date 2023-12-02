/*
Credit Card Transactions Queries

Skills used: Joins, CTE's, Date Functions, Windows Functions, Aggregate Functions

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
--,FORMAT(round(spends*100.0/total_spends,5),'0.#####') as percentage_contribution
,spends*100.0/total_spends as percentage_contribution
from c_ts),
rk_s as (select *,
rank() over(order by spends desc) as rnk
from p_cs)
select city,spends,percentage_contribution from rk_s where rnk<=5

--- query to print highest spend month and amount spent in that month for each card type
with ymc_a as (select
datepart(year,transaction_date) as transaction_year,datepart(month,transaction_date) as transaction_month,
card_type,sum(amount) as spend_month
from credit_card_transactions
group by datepart(year,transaction_date),datepart(month,transaction_date),card_type),
hsm as (select *,
sum(spend_month) over(partition by transaction_year,transaction_month order by spend_month desc rows between unbounded preceding and unbounded following) as highest_spend_month
from ymc_a)
select * from 
(select *,
dense_rank() over(order by highest_spend_month desc) as rnk
from hsm) A 
where rnk=1

-- query to print the transaction details(all columns from the table) for each card type when it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)
with rts as (select *,
sum(amount) over(partition by card_type order by amount rows between unbounded preceding and current row) as running_total_spends
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
with ccs as (select card_type,city,sum(amount) as spend
from credit_card_transactions
where card_type='Gold'
group by card_type,city),
ts as (select *,
sum(spend) over(order by spend rows between unbounded preceding and unbounded following) as total_spends
from ccs),
per_s as (select *
--,format(round(spend*100.0/total_spends,5),'0.#####') as percentage_spend
,spend*100.0/total_spends as percentage_spend
from ts)
select * from
(select *,
rank() over(order by percentage_spend) as rnk
from per_s) A
where rnk=1

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
with cte as (select gender,exp_type,sum(amount) as spends
from credit_card_transactions
where gender='F'
group by gender,exp_type),
cte2 as (select *,
sum(spends) over(order by spends rows between unbounded preceding and unbounded following) as total_spends
from cte)
select *,spends*100.0/total_spends as spends_percent_contribution from cte2

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
cte4 as (select *,(spends-prev_spend)*100.0/prev_spend as mom
from cte3
where yr_mth=201401)
select * from 
(select card_type,exp_type,yr_mth,mom,
rank() over(partition by card_type order by mom desc) as rnk
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