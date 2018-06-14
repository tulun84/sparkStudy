INSERT OVERWRITE TABLE daily_visit partition (date="${date}")
select sum(pv) pv,
count(distinct(guid)) uv,
count(case when pv>=2 then sessionid else null end) second_num,
count(sessionid) session_num
from (
select substr(ds,1,10) date,sessionid,max(guid) guid,count(url) pv from track_log
where ds="${date}" and hour="18" and length(url)>0
group by substr(ds,1,10),sessionid
) as s
 group by date