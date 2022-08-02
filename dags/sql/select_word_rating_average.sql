--KPI to see the avg of words for the day for each rating
select 
	'{{ params.metric }}' as metric,
	rating as variable, 
	now() as timestamp, 
	'{{ ds }}' as date,
	round(avg(cast(words as integer)),0) as value
from public.fanfictions
where download_date = '{{ ds }}'
group by rating