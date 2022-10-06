
select '{{ params.metric }}' as metric, 
		language as variable,
		now() as timestamp, 
		'{{ ds }}' as date, 
		count(*) as value
from public.fanfictions 
where download_date = '{{ ds }}'
group by language