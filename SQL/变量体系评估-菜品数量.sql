-- 变量体系
-- 菜品数量
select a.bm_waybill_id as bm_waybill_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count
from
(
	select first.bm_waybill_id as bm_waybill_id,order_detail.count as count
	from
	(select distinct bm_waybill_id
	from mart_peisongpa.fact_waybill_track_time 
	as track_time
	where dt dt>=$now.delta(7).datekey and dt<$now.datekey
	and bm_waybill_id is not null
	and ordertime is not null 
	and taketime is not null) 
	as first
	join
	(select id,platform_origin_order_id
	from mart_peisong.fact_waybill_pkg_info
	as pkg_info 
	where dt>=$now.delta(7).datekey and dt<$now.datekey) 
	as second
	on first.bm_waybill_id=second.id
	join
	origindb.waimai__wm_order_detail 
	as order_detail
	on second.platform_origin_order_id = order_detail.wm_order_id
)as a








SELECT *
FROM
(
  SELECT a.order_count,row_number() over (ORDER BY a.order_count DESC)as row_number
  FROM
  (		SELECT food_id ,order_count
        FROM mart_peisongpa_test.cook_time_per_food
        WHERE dt>0
  )as a
) as aa
WHERE aa.row_number=1000000


