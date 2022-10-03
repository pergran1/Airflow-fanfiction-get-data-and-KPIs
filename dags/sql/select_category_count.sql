select '{{ params.metric }}' as metric,
		category as variable,
		now() as timestamp, 
		'{{ ds }}' as date, 
		count(*) as value
from public.fanfictions 
where download_date = '{{ ds }}'
group by category