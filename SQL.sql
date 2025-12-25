create database Netflix; 

-- converting column data_aaded from text to date and release_year from text to date 
select * from netflix1;
alter table netflix1 
add column Date_added_to_date date ; 

set sql_safe_updates = 0 ; 

update netflix1 
	set Date_added_to_date = str_to_date(date_added, '%m/%d/%Y')
    where date_added is not null ; 

alter table netflix1 
add column year_release_to_year year; 

update netflix1
	set year_release_to_year = cast(release_year as unsigned); 

select * from netflix1; 

alter table netflix1 
drop date_added, 
drop release_year; 

select * from netflix1; 

-- cleaning country column (replacing the null value with 'Not Gien')
select coalesce(country, 'Not Given') as country 
from netflix1; 

-- features engineering and cleaning in the views  
create or replace view netflix_clean as 
select 
	show_id,type, title, director,country,rating , duration , listed_in ,
	2025- year_release_to_year as age_of_cotent, 
    case when year_release_to_year >= 2020 then 1 else 0 end as is_new_release,
    case when rating in ('TV-Y', 'TV-Y7', 'PG') then 1 else 0 end as is_kids_friendly, 
    case when rating in ('TV-MA', 'R') then 1 else 0 end as is_mature ,
    length(title) as title_length ,
    MONTH(Date_added_to_date) as month_added,
    DAYNAME(Date_added_to_date) as day_added
from netflix1; 

-- EDA 
select 
type, count(*) as title_count, 
avg(age_of_cotent) as Avg_Content_Age,
sum(is_kids_friendly) as Total_kind_friendly,
sum(is_mature) as total_mature
from netflix_clean 
group by type ;

-- simulate user and watch logs 
create table users(
 user_id int primary key ,
 age int ,
 country varchar(12),
 genre_pref varchar(25),
 ab_group char(2)
);

insert into users 
select row_number() over() as user_id, 
floor(18 + rand()*50) as age,
ELT(1 + floor(rand()*5), 'USA', 'UK','INDIA','BRAZIL','FRANCE') as country,
ELT(1 + floor(rand()*5), 'COMEDIES', 'DRAMAS','ACTION','HORROR','DOCUMENTARIES') as genre_pref,
if(rand()<0.5, 'A','B') as ab_group 
from netflix_clean
limit 10000; 

select * from users ;

create table watch_log (
user_id int ,
show_id varchar(15),
watch_min int ,
linked boolean,
watch_date date
);

insert into watch_log 
select 
u.user_id ,
c.show_id,
floor(30 + rand()*90) as watch_min,
case when rand()<0.7 then 1 else 0 end as linked,
date_add('2024-01-01', interval floor(rand()*90) day) as watch_date
from users u 
join netflix_clean c 
on instr(c.listed_in, u.genre_pref) > 0 limit 50000;

select * from watch_log;

-- A/B test (mature thumbnail vs generic)
create table ab_test as select 
w.user_id, 
w.show_id,
w.watch_min,
u.ab_group,
c.is_mature,
case when u.ab_group = 'B' and c.is_mature = 1 then 'mature_thumb' else 'generic_thumb' end as thumbnail_show
from watch_log w 
join users u on 
w.user_id = u.user_id
join netflix_clean c on 
w.show_id = c.show_id;

select thumbnail_show, 
count(*) as views , round(avg(watch_min),2) as avg_watch_min,
round(stddev(watch_min),2) as sd_watch_min
from ab_test 
group by thumbnail_show; 


-- CONTENT PERFORMANCE  (top / flop)
-- most & least watched titles
SELECT c.title,
       c.type,
       ROUND(SUM(w.watch_min),2) AS total_watch_min,
       COUNT(*)                  AS views,
       ROUND(AVG(w.watch_min),2) AS avg_watch_min
FROM netflix_clean c
JOIN watch_log w ON c.show_id = w.show_id
GROUP BY c.title, c.type
ORDER BY total_watch_min DESC
LIMIT 15;

-- bottom 10
SELECT c.title,
       c.type,
       ROUND(SUM(w.watch_min), 2) AS total_watch_min,
       COUNT(*)                   AS views,
       ROUND(AVG(w.watch_min), 2) AS avg_watch_min
FROM netflix_clean c
JOIN watch_log w ON c.show_id = w.show_id
GROUP BY c.title, c.type
ORDER BY total_watch_min ASC
LIMIT 10;

--  USER ENGAGEMENT BY SEGMENT
-- average watch time per age bucket
SELECT CASE WHEN age < 25 THEN '18-24'
            WHEN age < 35 THEN '25-34'
            WHEN age < 45 THEN '35-44'
            ELSE '45+' END AS age_bucket,
       ROUND(AVG(watch_min),2) AS avg_watch_min,
       COUNT(*)                AS total_views
FROM users u
JOIN watch_log w ON u.user_id = w.user_id
GROUP BY age_bucket
ORDER BY age_bucket;

-- GENRE POPULARITY  (simple LIKE filter)
SELECT g.genre,
       COUNT(*) AS watch_count,
       ROUND(AVG(w.watch_min),2) AS avg_watch_min
FROM (
      SELECT 'COMEDIES' AS genre UNION ALL
      SELECT 'DRAMAS' UNION ALL
      SELECT 'ACTION' UNION ALL
      SELECT 'HORROR' UNION ALL
      SELECT 'DOCUMENTARIES'
) g
JOIN netflix_clean c ON INSTR(UPPER(c.listed_in), g.genre) > 0
JOIN watch_log w ON c.show_id = w.show_id
GROUP BY g.genre
ORDER BY watch_count DESC;

--  DIRECTOR & COUNTRY INSIGHTS
-- top directors by total watch time
SELECT director,
       ROUND(SUM(w.watch_min),2) AS total_watch_min,
       COUNT(*)                  AS views
FROM netflix_clean c
JOIN watch_log w ON c.show_id = w.show_id
WHERE director <> 'Not Given'
GROUP BY director
ORDER BY total_watch_min DESC
LIMIT 10;

-- top countries
SELECT country,
       COUNT(*) AS titles,
       ROUND(AVG(age_of_cotent),1) AS avg_age
FROM netflix_clean
WHERE country <> 'Not Given'
GROUP BY country
ORDER BY titles DESC;

-- RETENTION / CHURN FLAG
CREATE OR REPLACE VIEW user_stats AS
SELECT user_id,
       COUNT(*)               AS total_views,
       ROUND(AVG(watch_min),2) AS avg_watch_min,
       MAX(watch_date)        AS last_watch,
       CASE WHEN AVG(watch_min) < 30 THEN 'Churned'
            ELSE 'Active' END AS status
FROM watch_log
GROUP BY user_id;

-- churn rate
SELECT status, COUNT(*) AS users
FROM user_stats
GROUP BY status;

--  TIME-BASED TRENDS
-- watch activity by calendar month
SELECT MONTH(watch_date) AS month_num,
       MONTHNAME(watch_date) AS month_name,
       COUNT(*) AS views,
       ROUND(AVG(watch_min),2) AS avg_watch_min
FROM watch_log
GROUP BY month_num, month_name
ORDER BY month_num;

-- day-of-week pattern
SELECT DAYNAME(watch_date) AS day_name,
       COUNT(*) AS views,
       ROUND(AVG(watch_min),2) AS avg_watch_min
FROM watch_log
GROUP BY day_name
ORDER BY FIELD(day_name,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');
-- delete 30 % of watch history randomly
DELETE FROM watch_log
WHERE rand() < 0.3;

-- checking the user unwatch 
SELECT COUNT(*) AS unseen_count
FROM users u
JOIN netflix_clean c ON INSTR(UPPER(c.listed_in), UPPER(u.genre_pref)) > 0
WHERE NOT EXISTS (
    SELECT 1
    FROM watch_log w
    WHERE w.user_id = u.user_id
      AND w.show_id = c.show_id
);

-- checking the user, title pair exists
SELECT COUNT(*) AS total_pairs
FROM users u
JOIN netflix_clean c ON INSTR(UPPER(c.listed_in), UPPER(u.genre_pref)) > 0;

-- checking watchlog existing  
SELECT COUNT(*) AS total_watches FROM watch_log;