/*
Olympic Games Data Analysis

Skills used: Joins, CTE's, Windows Functions, Aggregate Functions, Use cases

*/

-- team which has won the maximum gold medals over the years.
select top 1  team,count(medal) as gold_cnt from athlete_events ae
inner join athletes a on ae.athlete_id=a.id
where medal='Gold'
group by team
order by gold_cnt desc;


-- for each team printing total silver medals and year in which they won maximum silver medal..output 3 columns
-- team,total_silver_medals, year_of_max_silver
with cte as (select a.*,ae.games,ae.year as g_year,ae.season,ae.city,ae.sport,ae.event as g_event,ae.medal
from athletes a
inner join athlete_events ae on a.id=ae.athlete_id
where ae.medal='Silver'),
cte2 as (select team,g_year,count(distinct g_event) as silver_count 
from cte
group by team,g_year)
select team,total_silver_medals,g_year as year_of_max_silver from
(select *,
sum(silver_count) over(partition by team order by silver_count desc rows between unbounded preceding and unbounded following) as total_silver_medals,
rank() over(partition by team order by silver_count desc,g_year desc) as max_silver_rnk
from cte2) B
where max_silver_rnk=1


-- player who has won maximum gold medals  amongst the players 
--which have won only gold medal (never won silver or bronze) over the years
with cte as (select a.*,ae.games,ae.year as g_year,ae.season,ae.city,ae.sport,ae.event as g_event,ae.medal
from athletes a
inner join athlete_events ae on a.id=ae.athlete_id),
cte2 as (select id,name,
count(case when medal='Gold' then medal end) as gold_medal_count,
count(case when medal='Silver' then medal end) as silver_medal_count,
count(case when medal='Bronze' then medal end) as bronze_medal_count
from cte
group by id,name)
select * from
(select *,
dense_rank() over(order by gold_medal_count desc) as max_gold_rnk
from cte2
where silver_medal_count=0 and bronze_medal_count=0) C
where max_gold_rnk=1


--in each year player who has won maximum gold medal . Writing a query to print year,player name 
--and no of golds won in that year . In case of a tie print comma separated player names.
with cte as (select a.id as player_id,a.name as player_name,a.team,ae.year as g_year,ae.medal
from athletes a
inner join athlete_events ae on a.id=ae.athlete_id
where ae.medal='Gold'),
cte2 as (select g_year,player_id,player_name,count(medal) as gold_medal_count 
from cte
group by g_year,player_id,player_name),
cte3 as (select *,
dense_rank() over(partition by g_year order by gold_medal_count desc) as max_gold_rnk
from cte2)
select g_year,gold_medal_count as no_of_golds,
string_agg(player_name,',') within group(order by player_id asc) as player_names
from cte3
where max_gold_rnk=1
group by g_year,gold_medal_count
order by g_year asc,no_of_golds desc


--in which event and year India has won its first gold medal,first silver medal and first bronze medal
--printing 3 columns medal,year,sport
with cte as (select a.id,a.name as player_name,a.team,ae.year as g_year,ae.sport,ae.event as g_event,ae.medal
from athletes a
inner join athlete_events ae on a.id=ae.athlete_id
where a.team='India' and ae.medal!='NA')
select medal,g_year,sport,g_event,first_medal_year_rnk from
(select *,
row_number() over(partition by medal order by g_year) as first_medal_year_rnk
from cte) D
where first_medal_year_rnk=1


-- players who won gold medal in summer and winter olympics both.
with cte as (select a.id as player_id,a.name as player_name,ae.season,ae.medal
from athletes a
inner join athlete_events ae on a.id=ae.athlete_id
where ae.medal='Gold' and ae.season in ('Summer','Winter')),
cte2 as (select player_id,player_name,
count(case when season='Summer' then medal end) as summer_gold_count,
count(case when season='Winter' then medal end) as winter_gold_count
from cte
group by player_id,player_name)
select * 
from cte2
where summer_gold_count>=1 and winter_gold_count>=1


-- players who won gold, silver and bronze medal in a single olympics. printing player name along with year.
with cte as (select a.id as player_id,a.name as player_name,ae.games,ae.year as g_year,ae.season,ae.medal
from athletes a
inner join athlete_events ae on a.id=ae.athlete_id
where ae.medal!='NA'),
cte2 as (select player_id,player_name,g_year,season,
count(case when medal='Gold' then medal end) as gold_medal_count,
count(case when medal='Silver' then medal end) as silver_medal_count,
count(case when medal='Bronze' then medal end) as bronze_medal_count
from cte
group by player_id,player_name,g_year,season)
select player_id,player_name,g_year,gold_medal_count,silver_medal_count,bronze_medal_count 
from cte2 
where gold_medal_count>=1 and silver_medal_count>=1 and bronze_medal_count>=1


-- players who have won gold medals in consecutive 3 summer olympics in the same event . Considering only olympics 2000 onwards. 
--Assuming summer olympics happens every 4 year starting 2000 & hence, printing player name and event name.
with cte as (select a.id as player_id,a.name as player_name,ae.year as g_year,ae.season,ae.event as g_event,ae.medal
from athletes a
inner join athlete_events ae on a.id=ae.athlete_id
where ae.medal='Gold' and ae.season='Summer' and ae.year>=2000)
select player_id,player_name,season,g_event,medal,same_event_consecutive_yr_rnk from 
(select *,
dense_rank() over(partition by player_id,player_name,g_event order by g_year asc) as same_event_consecutive_yr_rnk
from cte) E
where same_event_consecutive_yr_rnk=3

