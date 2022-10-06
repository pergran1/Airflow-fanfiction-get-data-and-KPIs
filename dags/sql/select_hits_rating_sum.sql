select
		'{{ params.metric }}' as metric,
		rating as variable, 
		now() as timestamp, 
		'{{ ds }}' as date,
		round(sum(cast(hits as integer)),0) as value
from public.fanfictions
	where download_date = '{{ ds }}'
	group by rating