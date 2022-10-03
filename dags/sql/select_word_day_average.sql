select
		'{{ params.metric }}' as metric,
		download_date as variable, 
		now() as timestamp, 
		'{{ ds }}' as date,
		round(avg(cast(words as integer)),0) as value
from public.fanfictions
	where download_date = '{{ ds }}'
	group by download_date