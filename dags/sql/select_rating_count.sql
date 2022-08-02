--enligt pricerunners template 
select '{{ params.metric }}' as metric, 
		rating as variable,   --pricerunner has country or category instead
		now() as timestamp, 
		'{{ ds }}' as date, 
		count(*) as value
from public.fanfictions 
where download_date = '{{ ds }}'
group by rating