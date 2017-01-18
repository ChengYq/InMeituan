-- 计算稳定性
-- ETL跑出最近七天的数据 ,每个7天，都是根据这一天之前的那个7七天的数据得出来的，也就是“7个7天”
-- 现在需要计算出每个菜品，在最近七天的预估时间的均值和方差

select food_id ,cook_time,dt
from 
(
	select *
	from mart_peisongpa_test.cook_time_per_food
	where dt>= '20160913' and dt <= '20160919'
)as together
order by food_id
limit 10000



-- 以下代码是求7天测试的均值和方差的
select a.f_id as food_id,avg(a.c_time) as average_value,stddev_pop(a.c_time)as stddev_value
from
(
select food_id as f_id,cook_time as c_time
from mart_peisongpa_test.cook_time_per_food
where dt>= '20160915' and dt <= '20160921'
)as a
GROUP BY a.f_id
LIMIT 10000





-- 以下代码是用来评估20160926的新代码效果的
-- 画图：横轴是time_level ，纵轴是某个订单花费的时间
-- mart_peisongpa.bm_delivery_wm_order_detail 订单中的food_id从这里取
-- mart_peisongpa.fact_waybill_track_time 每个订单的实际用时从这取

select f.bm_waybill_id as bm_waybill_id,f.consume_time as consume_time,sum(my.time_level) as sum_time_level,avg(my.time_level) as avg_time_level, max(my.time_level) as max_time_level,sum(my.cook_time) as sum_cook_time,avg(my.cook_time) as avg_cook_time,max(my.cook_time) as max_cook_time
from
(
		select first.bm_waybill_id as bm_waybill_id,first.taketime-first.ordertime as consume_time,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name
		from
		(select *
		from mart_peisongpa.fact_waybill_track_time as track_time
		where dt='20161018' and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
		join
		(select *
		from mart_peisong.fact_waybill_pkg_info as pkg_info 
		where dt='20161018') as second
		on first.bm_waybill_id=second.id
		join
		origindb.waimai__wm_order_detail as order_detail
		on second.platform_origin_order_id = order_detail.wm_order_id
)as f
join 
(
	select *
	from mart_peisongpa_test.cook_time_per_food 
	where dt='20161018'
)
as my on f.food_id=my.food_id
group by f.bm_waybill_id,f.consume_time
limit 10000






-- 评估更新版本
-- 归一化菜品
-- 20161019更新
select 
f.bm_waybill_id as bm_waybill_id
,f.consume_time as consume_time
,sum(my.time_level) as sum_time_level
,avg(my.time_level) as avg_time_level
,max(my.time_level) as max_time_level
,sum(my.cook_time) as sum_cook_time
,avg(my.cook_time) as avg_cook_time
,max(my.cook_time) as max_cook_time
from
(
	select time_level, cook_time,rk_grp
	from mart_peisongpa_test.cook_time_per_food 
	where dt='20161018'
)
as my 
join
(
	select first.bm_waybill_id as bm_waybill_id
	,first.taketime-first.ordertime as consume_time
	,first.platform_poi_id as poi_id
	,order_detail.wm_order_id as wm_order_id
	,order_detail.wm_food_id as food_id
	,order_detail.food_name as food_name
	,general2origin.rk_grp as rk_grp
	from
		(
		select bm_waybill_id, taketime, ordertime, platform_poi_id
		from mart_peisongpa.fact_waybill_track_time as track_time
		where dt='20161018' 
			and bm_waybill_id is not null 
			and ordertime is not null 
			and taketime is not null
		) as first
	join
		(
		select id, platform_origin_order_id
		from mart_peisong.fact_waybill_pkg_info as pkg_info 
		where dt='20161018'
		)as second
	on first.bm_waybill_id=second.id
	join
	origindb.waimai__wm_order_detail as order_detail
	on 
	second.platform_origin_order_id = order_detail.wm_order_id
	join
	mart_waimaigrowth.food_general2origin as general2origin
	on order_detail.wm_food_id =  general2origin.wm_food_id
)as f
on f.rk_grp=my.rk_grp
group by f.bm_waybill_id,f.consume_time
limit 10000



