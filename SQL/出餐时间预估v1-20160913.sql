-- 表： mart_peisongpa.bm_delivery_wm_order_detail  :wm_food_id, food_count, bm_waybill_id
-- wm_food_id: 菜品id
-- food_count: 主菜个数
-- bm_waybill_id: 运单id

-- 表： mart_shangchao.fact_waybill_expand :pushtime, fetchtime, id, poi_id
-- pushtime：商家推单时间
-- fetchtime 骑手取货时间
-- id 运单id 
-- poi_id 门店id
select wm_food_id,to_utc_timestamp(shangchao.fetchtime,'UTC'),to_utc_timestamp(shangchao.pushtime,'UTC'),shangchao.poi_id
from mart_peisongpa.bm_delivery_wm_order_detail as peisong
join mart_shangchao.fact_waybill_expand as shangchao
on peisong.bm_waybill_id=shangchao.id
where peisong.dt='20160911' and shangchao.dt='20160911'



-- unix_timestamp()函数是用来计算参数时间到19700101 00：00：00的时间
-- unix_timestamp()函数，返回值的以秒作为单位的
select peisong.wm_food_id,(unix_timestamp(shangchao.fetchtime)-unix_timestamp(shangchao.pushtime))/(peisong.food_count),shangchao.poi_id
from mart_peisongpa.bm_delivery_wm_order_detail as peisong
join mart_shangchao.fact_waybill_expand as shangchao
on peisong.bm_waybill_id=shangchao.id
where peisong.dt='20160911' and shangchao.dt='20160911'


-- 以下代码是用来实现功能的，具体来说是两个select的嵌套实现
-- 必须后边不加上a.shangjia_id，否则报错。具体的原因参加group by的用法
select  a.food_id,avg(a.aver_in_order)
from 
(select wm_food_id as food_id,(unix_timestamp(shangchao.fetchtime)-unix_timestamp(shangchao.pushtime))/(peisong.food_count) as aver_in_order,shangchao.poi_id as shangjia_id
from mart_peisongpa.bm_delivery_wm_order_detail as peisong
join mart_shangchao.fact_waybill_expand as shangchao
on peisong.bm_waybill_id=shangchao.id
where peisong.dt='20160911' and shangchao.dt='20160911'
)as a
group by a.food_id


-- 以下代码是完善了上述代码，在最后一列加入了商家ID
-- food_id 更加细粒度一些
select  a.food_id,avg(a.aver_in_order),a.poi_id
from 
(select wm_food_id as food_id,(unix_timestamp(shangchao.fetchtime)-unix_timestamp(shangchao.pushtime))/(peisong.food_count) as aver_in_order,shangchao.poi_id as poi_id
from mart_peisongpa.bm_delivery_wm_order_detail as peisong
join mart_shangchao.fact_waybill_expand as shangchao
on peisong.bm_waybill_id=shangchao.id
where peisong.dt='20160911' and shangchao.dt='20160911'
)as a
group by a.food_id,a.poi_id




-- 以下代码修改了时间，将时间变成了一周的,一般可以获得13w条左右的数据
select  a.food_id,avg(a.aver_in_order),a.poi_id
from 
(select peisong.wm_food_id as food_id,(unix_timestamp(shangchao.fetchtime)-unix_timestamp(shangchao.pushtime))/(peisong.food_count) as aver_in_order,shangchao.poi_id as poi_id
from mart_peisongpa.bm_delivery_wm_order_detail as peisong

join mart_shangchao.fact_waybill_expand as shangchao
on peisong.bm_waybill_id=shangchao.id
where peisong.dt between '20160911' and '20160918' and shangchao.dt between '20160911' and '20160918'
)as a
group by a.food_id,a.poi_id



-- 再次修改代码
-- 1. 修改查询逻辑，更加高效
-- 2. 换了一张表： mart_peisongpa.fact_waybill_track_time, 替换了原来的
--    需要用到的字段：wm_food_id foodId   ordertime 商家接单时间  arrivetime 骑手到店时间

select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id
from
(
	select first.wm_food_id as food_id,(second.arrivetime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
		from
		(
			select * 
			from
			mart_peisongpa.bm_delivery_wm_order_detail as order_detail
			where dt between '20160911' and '20160918'
		)   as first
		join 
		(	select *
			from
			mart_peisongpa.fact_waybill_track_time as track_time
			where dt between '20160911' and '20160918'
		)as second
		on 
		first.bm_waybill_id=second.bm_waybill_id
)as a
group by a.food_id,a.poi_id




-- 再次修改代码
-- food_id 加入属性控制，不允许为控制(ETL中，在建表的时候就加入条件控制)
-- 计算时间的时候，对于为空的数据进行了跳过
select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id
from
(
	select first.wm_food_id as food_id,(second.arrivetime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
		from
		(
			select * 
			from
			mart_peisongpa.bm_delivery_wm_order_detail as order_detail
			where dt between '20160911' and '20160918' and wm_food_id is not null and food_count is not null
		)   as first
		join 
		(	select *
			from
			mart_peisongpa.fact_waybill_track_time as track_time
			where dt between '20160911' and '20160918' and arrivetime is not null and ordertime is not null and platform_poi_id is not null
		)as second
		on 
		first.bm_waybill_id=second.bm_waybill_id
)as a
group by a.food_id,a.poi_id



-- 修改代码
-- 输出计算每个cook_time时用到的订单数量
select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id,count(*)
from
(
	select first.wm_food_id as food_id,(second.arrivetime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
		from
		(
			select * 
			from
			mart_peisongpa.bm_delivery_wm_order_detail as order_detail
			where dt='20160919' and wm_food_id is not null and food_count is not null
		)   as first
		join 
		(	select *
			from
			mart_peisongpa.fact_waybill_track_time as track_time
			where dt='20160919' and arrivetime is not null and ordertime is not null and platform_poi_id is not null
		) as second
		on 
		first.bm_waybill_id=second.bm_waybill_id
)as a
group by a.food_id,a.poi_id





-- arrivetime->taketime
select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id,count(*)
from
(
	select first.wm_food_id as food_id,(second.taketime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
		from
		(
			select * 
			from
			mart_peisongpa.bm_delivery_wm_order_detail as order_detail
			where dt='20160919' and wm_food_id is not null and food_count is not null
		)   as first
		join 
		(	select *
			from
			mart_peisongpa.fact_waybill_track_time as track_time
			where dt='20160919' and taketime is not null and ordertime is not null and platform_poi_id is not null
		)as second
		on 
		first.bm_waybill_id=second.bm_waybill_id
)as a
group by a.food_id,a.poi_id







-- 重要更新！！！
-- 粗粒度的分类！！上周周会之后，重新修改
-- 将 cook_time 进行粗粒度的分类，也就是对于不同的 food_id，它们的cook_time取值不再是连续的，而是离散的
-- 看图分析之后，将送餐时间分成5段，0~50，50~350，305~850，850~1200，1200~2000，2000+，每一段的time使用该区间的平均数代替
-- 首先实现分类，然后计算平均数
select  
aa.food_id,
case 
	when aa.cook_time <= 50 then 0
	when aa.cook_time >= 50 and aa.cook_time< 350 then 1
	when aa.cook_time >= 350 and aa.cook_time< 850 then 2
	when aa.cook_time >= 850 and aa.cook_time< 1200 then 3
	when aa.cook_time >= 1200 and aa.cook_time< 2000 then 4
	else 5
end as time_level,
aa.poi_id,
aa.order_count
from
	(
	select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id,count(*) as order_count
	from
	(
		select first.wm_food_id as food_id,(second.taketime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
			from
			(
				select * 
				from
				mart_peisongpa.bm_delivery_wm_order_detail as order_detail
				where dt='20160925' and wm_food_id is not null and food_count is not null
			)   as first
			join 
			(	select *
				from
				mart_peisongpa.fact_waybill_track_time as track_time
				where dt='20160925' and taketime is not null and ordertime is not null and platform_poi_id is not null
			)as second
			on 
			first.bm_waybill_id=second.bm_waybill_id
	)as a
	group by a.food_id,a.poi_id
	) as aa




-- 继续完善上述代码，能够输出平均数
-- 这个代码有bug，逻辑上的错误
select  
aa.food_id,
case 
	when aa.cook_time <= 50 then 0
	when aa.cook_time >= 50 and aa.cook_time< 350 then 1
	when aa.cook_time >= 350 and aa.cook_time< 850 then 2
	when aa.cook_time >= 850 and aa.cook_time< 1200 then 3
	when aa.cook_time >= 1200 and aa.cook_time< 2000 then 4
	else 5
end as time_level,
avg(aa.cook_time)
-- aa.poi_id,
-- aa.order_count
from
	(
	select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id,count(*) as order_count
	from
	(
		select first.wm_food_id as food_id,(second.taketime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
			from
			(
				select * 
				from
				mart_peisongpa.bm_delivery_wm_order_detail as order_detail
				where dt='20160925' and wm_food_id is not null and food_count is not null
			)   as first
			join 
			(	select *
				from
				mart_peisongpa.fact_waybill_track_time as track_time
				where dt='20160925' and taketime is not null and ordertime is not null and platform_poi_id is not null
			)as second
			on 
			first.bm_waybill_id=second.bm_waybill_id
	)as a
	group by a.food_id,a.poi_id
	) as aa
group by
case 
	when aa.cook_time <= 50 then 0
	when aa.cook_time >= 50 and aa.cook_time< 350 then 1
	when aa.cook_time >= 350 and aa.cook_time< 850 then 2
	when aa.cook_time >= 850 and aa.cook_time< 1200 then 3
	when aa.cook_time >= 1200 and aa.cook_time< 2000 then 4
	else 5
end as time_level,
aa.food_id
limit 1000



-- 20160927的代码
-- 优化了代码的查询逻辑
-- over(partition by) 的用法很重要！！区分和group by 的区别
SELECT *
FROM
(select 
	aaa.food_id as food_id,
	aaa.time_level as time_level,
	avg(aaa.cook_time) over (partition by aaa.time_level) as aver_time,
	aaa.poi_id as poi_id
from(
select  
aa.food_id as food_id,
case 
	when aa.cook_time <= 50 then 0
	when aa.cook_time >= 50 and aa.cook_time< 350 then 1
	when aa.cook_time >= 350 and aa.cook_time< 850 then 2
	when aa.cook_time >= 850 and aa.cook_time< 1200 then 3
	when aa.cook_time >= 1200 and aa.cook_time< 2000 then 4
	else 5
end as time_level,
aa.cook_time as cook_time,
aa.poi_id as poi_id,
aa.order_count as order_count
from
	(
	select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id,count(*) as order_count
	from
	(
		select first.wm_food_id as food_id,(second.taketime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
			from
			(
				select * 
				from
				mart_peisongpa.bm_delivery_wm_order_detail as order_detail
				where dt='20160925' and wm_food_id is not null and food_count is not null
			)   as first
			join 
			(	select *
				from
				mart_peisongpa.fact_waybill_track_time as track_time
				where dt='20160925' and taketime is not null and ordertime is not null and platform_poi_id is not null
			)as second
			on 
			first.bm_waybill_id=second.bm_waybill_id
	)as a
	group by a.food_id,a.poi_id
	) as aa
	)as aaa
 )as t
ORDER BY t.food_id
LIMIT 10000





-- 重大改版！！！
-- 用origindb.waimai__wm_order_detail这个表，替换掉 mart_peisongpa.bm_delivery_wm_order_detail 
-- origindb.waimai__wm_order_detail 里边，同一个订单中的不同菜品，其中count 这个字段，表示的是该菜品有多少个，而不是该订单一共订了多少个，该菜品一共点的菜品数目，等于SUM(种类✖️个数)
-- 用到的表：
-- mart_peisongpa.fact_waybill_track_time（分区，dt）
-- 需要用到的字段：bm_waybill_id  运单ID  ordertime 商家接单时间  taketime 骑手取货时间

-- origindb.waimai__wm_order_detail (不是分区表)
-- 需要用到的字段：wm_order_id 外卖订单ID，wm_food_id 菜品ID，count：表示的是该菜品有多少个，而不是该订单一共订了多少个，该菜品一共点的菜品数目，等于SUM(种类✖️个数)

-- mart_peisong.fact_waybill_pkg_info (分区，dt)
-- 这个表建立了上边两个表的连接(建立了运单和订单之间的关系)
-- 其中，id=fact_waybill_track_time.bm_waybill_id
-- platform_origin_order_id=waimai__wm_order_detail.wm_order_id


-- 第一次迭代：
-- 生成表 字段分别是 外卖ID 运单ID food_id 该类food的ID  poi id



select first.bm_waybill_id,first.ordertime,first.taketime,first.platform_poi_id,order_detail.wm_order_id,order_detail.wm_food_id,order_detail.food_name,order_detail.count
from
	(select *
	from mart_peisongpa.fact_waybill_track_time as track_time
	where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
	join
	(select *
	from mart_peisong.fact_waybill_pkg_info as pkg_info 
	where dt='20160926') as second
	on first.bm_waybill_id=second.id
	join
	origindb.waimai__wm_order_detail as order_detail
	on second.platform_origin_order_id = order_detail.wm_order_id




-- 第二次迭代
select a.bm_waybill_id,a.wm_order_id,a.food_id,a.food_name,a.poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/count(*) over(partition by a.bm_waybill_id)
from
(
	select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,order_detail.count as count
from
	(select *
	from mart_peisongpa.fact_waybill_track_time as track_time
	where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
	join
	(select *
	from mart_peisong.fact_waybill_pkg_info as pkg_info 
	where dt='20160926') as second
	on first.bm_waybill_id=second.id
	join
	origindb.waimai__wm_order_detail as order_detail
	on second.platform_origin_order_id = order_detail.wm_order_id
)as a




-- 第三次迭代
select aa.food_id,aa.food_name,avg(aver_in_order) as cook_time,aa.poi_id
from
(
	select a.bm_waybill_id as bm_waybill_id,a.wm_order_id as wm_order_id,a.food_id as food_id,a.food_name as food_name,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/count(*) over(partition by a.bm_waybill_id) as aver_in_order
	from
	(
		select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,order_detail.count as count
	from
		(select *
		from mart_peisongpa.fact_waybill_track_time as track_time
		where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
		join
		(select *
		from mart_peisong.fact_waybill_pkg_info as pkg_info 
		where dt='20160926') as second
		on first.bm_waybill_id=second.id
		join
		origindb.waimai__wm_order_detail as order_detail
		on second.platform_origin_order_id = order_detail.wm_order_id
	)as a
)as aa
group by aa.food_id,aa.food_name
limit 1000




-- 第四次迭代
select  aaa.food_id as food_id,
aaa.food_name as food_name,
case 
	when aaa.cook_time <= 50 then 0
	when aaa.cook_time >= 50 and aaa.cook_time< 350 then 1
	when aaa.cook_time >= 350 and aaa.cook_time< 850 then 2
	when aaa.cook_time >= 850 and aaa.cook_time< 1200 then 3
	when aaa.cook_time >= 1200 and aaa.cook_time< 2000 then 4
	else 5
end as time_level,
aaa.poi_id as poi_id
from
(
	select aa.food_id as food_id,aa.food_name as food_name,avg(aver_in_order) as cook_time,aa.poi_id as poi_id
	from
	(
		select a.bm_waybill_id as bm_waybill_id,a.wm_order_id as wm_order_id,a.food_id as food_id,a.food_name as food_name,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/count(*) over(partition by a.bm_waybill_id) as aver_in_order
		from
		(
			select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,order_detail.count as count
		from
			(select *
			from mart_peisongpa.fact_waybill_track_time as track_time
			where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
			join
			(select *
			from mart_peisong.fact_waybill_pkg_info as pkg_info 
			where dt='20160926') as second
			on first.bm_waybill_id=second.id
			join
			origindb.waimai__wm_order_detail as order_detail
			on second.platform_origin_order_id = order_detail.wm_order_id
		)as a
	)as aa
	group by aa.food_id,aa.food_name,aa.poi_id
) as aaa
limit 1000



-- 第五次迭代
select 
aaaa.food_id as food_id,
aaaa.food_name as food_name,
avg(aaaa.cook_time) over (partition by aaaa.time_level) as cook_time,
aaaa.poi_id as poi_id
from
(
	select  aaa.food_id as food_id,
	aaa.food_name as food_name,
	aaa.cook_time as cook_time,
	case 
		when aaa.cook_time <= 50 then 0
		when aaa.cook_time >= 50 and aaa.cook_time< 350 then 1
		when aaa.cook_time >= 350 and aaa.cook_time< 850 then 2
		when aaa.cook_time >= 850 and aaa.cook_time< 1200 then 3
		when aaa.cook_time >= 1200 and aaa.cook_time< 2000 then 4
		else 5
	end as time_level,
	aaa.poi_id as poi_id
	from
	(
		select aa.food_id as food_id,aa.food_name as food_name,avg(aver_in_order) as cook_time,aa.poi_id as poi_id 
		from
		(
			select a.bm_waybill_id as bm_waybill_id,a.wm_order_id as wm_order_id,a.food_id as food_id,a.food_name as food_name,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/count(*) over(partition by a.bm_waybill_id) as aver_in_order
			from
			(
				select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,order_detail.count as count
				from
				(select *
				from mart_peisongpa.fact_waybill_track_time as track_time
				where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
				join
				(select *
				from mart_peisong.fact_waybill_pkg_info as pkg_info 
				where dt='20160926') as second
				on first.bm_waybill_id=second.id
				join
				origindb.waimai__wm_order_detail as order_detail
				on second.platform_origin_order_id = order_detail.wm_order_id
			)as a
		)as aa
		group by aa.food_id,aa.food_name,aa.poi_id
	) as aaa
)as aaaa
limit 1000




-- 第六次迭代
-- 增加一个字段：每个food_cook_time 是基于多少个历史订单计算出出来的（冷门菜品单独处理的先行工作）
select 
aaaa.food_id as food_id,
aaaa.food_name as food_name,
avg(aaaa.cook_time) over (partition by aaaa.time_level) as cook_time,
aaaa.poi_id as poi_id,
aaaa.order_count as order_count
from
(
	select  aaa.food_id as food_id,
	aaa.food_name as food_name,
	aaa.cook_time as cook_time,
	case 
		when aaa.cook_time <= 50 then 0
		when aaa.cook_time >= 50 and aaa.cook_time< 350 then 1
		when aaa.cook_time >= 350 and aaa.cook_time< 850 then 2
		when aaa.cook_time >= 850 and aaa.cook_time< 1200 then 3
		when aaa.cook_time >= 1200 and aaa.cook_time< 2000 then 4
		else 5
	end as time_level,
	aaa.poi_id as poi_id,
	aaa.order_count as order_count
	from
	(
		select aa.food_id as food_id,aa.food_name as food_name,avg(aver_in_order) as cook_time,aa.poi_id as poi_id, count(aa.food_id) as order_count
		from
		(
			select a.bm_waybill_id as bm_waybill_id,a.wm_order_id as wm_order_id,a.food_id as food_id,a.food_name as food_name,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/count(*) over(partition by a.bm_waybill_id) as aver_in_order
			from
			(
				select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,order_detail.count as count
				from
				(select bm_waybill_id,ordertime,taketime,platform_poi_id
				from mart_peisongpa.fact_waybill_track_time as track_time
				where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) 
				as first
				join
				(select platform_origin_order_id,id
				from mart_peisong.fact_waybill_pkg_info as pkg_info 
				where dt='20160926') 
				as second
				on first.bm_waybill_id=second.id
				join
				origindb.waimai__wm_order_detail as order_detail
				on second.platform_origin_order_id = order_detail.wm_order_id
			)as a
		)as aa
		group by aa.food_id,aa.food_name,aa.poi_id
	) as aaa
)as aaaa
limit 1000



-------------------------------------------------------------------
-------------------------------以下是归一化菜品---------------------
-------------------------------------------------------------------


-- 出餐时间新的大版本变更
-- 对于food_id , 全部都采用归一化的菜品ID
-- 使用线上表：mart_waimaigrowth.food_general2origin 
	-- 其中，需要我们用的字段是：wm_food_id 菜品ID
	-- food_name 原始的菜品名称
	-- food_name_clean 清洗了之后的菜品名称（对我们的业务没啥用，主要是对名称的清洗，不是归一化）
	-- food_stem 归一化后的菜品名称 
	-- rk_grp 归一化的ID，一个归一化名称一个ID，也就是多个food_id,可能会对应一个rk_grp
-- 需要评估四个版本 1. 归一化前 food_count 2. 归一化前 kind_count   3.归一化后 food_count  4. 归一化后 kind_count 
-- 归一化之前的两个版本 只需要改一下kind_count 和 food_count 就行了，以下代码的上半部分，就是用来修改kind_count 和 food_count 的
-- 下边重点是调整成归一化版本，重点是，查mart_waimaigrowth.food_general2origin， 把我们自己的food_id 调整成他们的ID

-- select 
-- aaaa.food_id as food_id,
-- aaaa.food_name as food_name,
-- avg(aaaa.cook_time) over (partition by aaaa.time_level) as cook_time,
-- aaaa.poi_id as poi_id,
-- aaaa.order_count as order_count
-- from
-- (
-- 	select  aaa.food_id as food_id,
-- 	aaa.food_name as food_name,
-- 	aaa.cook_time as cook_time,
-- 	case 
-- 		when aaa.cook_time <= 50 then 0
-- 		when aaa.cook_time >= 50 and aaa.cook_time< 350 then 1
-- 		when aaa.cook_time >= 350 and aaa.cook_time< 850 then 2
-- 		when aaa.cook_time >= 850 and aaa.cook_time< 1200 then 3
-- 		when aaa.cook_time >= 1200 and aaa.cook_time< 2000 then 4
-- 		else 5
-- 	end as time_level,
-- 	aaa.poi_id as poi_id,
-- 	aaa.order_count as order_count
-- 	from
-- 	(
-- 		select aa.food_id as food_id,aa.food_name as food_name,avg(aver_in_order) as cook_time,aa.poi_id as poi_id, count(aa.food_id) as order_count
-- 		from
-- 		(
-- 			select a.bm_waybill_id as bm_waybill_id,a.wm_order_id as wm_order_id,a.food_id as food_id,a.food_name as food_name,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/sum(a.count) over(partition by a.bm_waybill_id) as aver_in_order
-- 			from
-- 			(
-- 				select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,order_detail.count as count
-- 				from
-- 				(select *
-- 				from mart_peisongpa.fact_waybill_track_time as track_time
-- 				where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
-- 				join
-- 				(select *
-- 				from mart_peisong.fact_waybill_pkg_info as pkg_info 
-- 				where dt='20160926') as second
-- 				on first.bm_waybill_id=second.id
-- 				join
-- 				origindb.waimai__wm_order_detail as order_detail
-- 				on second.platform_origin_order_id = order_detail.wm_order_id
-- 			)as a
-- 		)as aa
-- 		group by aa.food_id,aa.food_name,aa.poi_id
-- 	) as aaa
-- )as aaaa
-- limit 1000



-- -------------------------------
-- -------------------------------
-- select distinct
-- aaaaa.rk_grp,
-- aaaaa.cook_time,
-- aaaaa.time_level
-- from
-- (
-- select
-- aaaa.rk_grp as rk_grp,
-- avg(aaaa.cook_time) over (partition by aaaa.time_level) as cook_time,
-- aaaa.time_level as time_level,
-- aaaa.poi_id as poi_id
-- from
-- (
-- 	select  
-- 	aaa.rk_grp as rk_grp,
-- 	aaa.cook_time as cook_time,
-- 	case 
-- 		when aaa.cook_time <= 50 then 0
-- 		when aaa.cook_time >= 50 and aaa.cook_time< 350 then 1
-- 		when aaa.cook_time >= 350 and aaa.cook_time< 850 then 2
-- 		when aaa.cook_time >= 850 and aaa.cook_time< 1200 then 3
-- 		when aaa.cook_time >= 1200 and aaa.cook_time< 2000 then 4
-- 		else 5
-- 	end as time_level,
-- 	aaa.poi_id as poi_id,
-- 	aaa.order_count as order_count
-- 	from
-- 	(
-- 		select aa.rk_grp as rk_grp,avg(aver_in_order) as cook_time,aa.poi_id as poi_id, count(distinct aa.rk_grp) as order_count
-- 		from
-- 		(
-- 			select a.bm_waybill_id as bm_waybill_id,a.rk_grp as rk_grp,a.wm_order_id as wm_order_id,a.poi_id as poi_id, count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/count(a.rk_grp) over(partition by a.bm_waybill_id) as aver_in_order
-- 			from
-- 			(
-- 				select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_order_id as wm_order_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,general2origin.rk_grp as rk_grp,order_detail.count as count
-- 				from
-- 					(select bm_waybill_id,ordertime,taketime,platform_poi_id
-- 					from mart_peisongpa.fact_waybill_track_time as track_time
-- 					where dt='20160926' and bm_waybill_id is not null and ordertime is not null and taketime is not null) 
-- 					as first
-- 					join
-- 					(select id,platform_origin_order_id
-- 					from mart_peisong.fact_waybill_pkg_info as pkg_info 
-- 					where dt='20160926')
-- 					as second
-- 					on first.bm_waybill_id=second.id
-- 					join
-- 					origindb.waimai__wm_order_detail as order_detail
-- 					on second.platform_origin_order_id = order_detail.wm_order_id
-- 					join
-- 					(   select rk_grp,wm_food_id
-- 						from mart_waimaigrowth.food_general2origin
-- 						where rk_grp is not null
-- 					)as general2origin
-- 					on order_detail.wm_food_id =  general2origin.wm_food_id
-- 			)as a
-- 		)as aa
-- 		group by aa.rk_grp,aa.poi_id
-- 	) as aaa
-- )as aaaa
-- ) as aaaaa

-- limit 1000

---------------------------------------------------------------------------------
-------分割线，以上部分代码可能不一定有效，主要是对该算法的评估结果比较差----------------
---------------------------------------------------------------------------------



-- 



