--enligt pricerunners template 
select '{{ params.metric }}' as metric, 
		rating as variable,   --pricerunner has country or category instead
		now() as timestamp, 
		'{{ ds }}' as date, 
		count(*) as value
from public.fanfictions 
group by rating, download_date