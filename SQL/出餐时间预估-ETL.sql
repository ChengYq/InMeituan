-- 第一个版本，sql语句是从《出餐时间预估v1-20160913.sql》中迭代出来的

##-- 2016.9.19 创建ETL

##Description##
##-- 本ETL用来产生针对每个菜品的出餐时间的预估

##TaskInfo##
creator = 'chengyuanqi@meituan.com'

source = {
    'db': META['hmart_peisongpa'],
}

stream = {
    'format': '',
}

target = {
    'db': META['hmart_peisongpa'],
    'table': 'cook_time_per_food',
}

##Extract##
##-- Extract节点, 这里填写一个能在source.db上执行的sql


##Preload##
#if $isCLEANUP
  drop table if exists `$target.table`
#end if


##Load##
##-- Load节点, (可以留空)
SET hive.exec.parallel=true;

#if  $isRELOAD

    SET mapred.reduce.tasks=490;

#end if
SET mapred.reduce.tasks=490;

INSERT overwrite TABLE `$target.table` partition(dt=$now.datekey)
select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id
from
(
	select first.wm_food_id as food_id,(second.arrivetime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
		from
		(
			select * 
			from
			mart_peisongpa.bm_delivery_wm_order_detail as order_detail
			where dt>=$now.delta(7).datekey and dt<$now.datekey and wm_food_id is not null and food_count is not null
		)   as first
		join 
		(	select *
			from
			mart_peisongpa.fact_waybill_track_time as track_time
			where dt>=$now.delta(7).datekey and dt<$now.datekey and arrivetime is not null and ordertime is not null and platform_poi_id is not null
		)as second
		on 
		first.bm_waybill_id=second.bm_waybill_id
)as a
group by a.food_id,a.poi_id

##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `food_id`         BIGINT     NOT NULL COMMENT '菜品ID',
  `cook_time`       BIGINT     NOT NULL  COMMENT '出餐时间',
  `poi_id`          BIGINT     NOT NULL  COMMENT '商家ID'
)COMMENT '用于进行菜品出餐时间预估的表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC









-- 第二个版本，
-- 变化：在计算每个菜品的出餐时间，输出它是基于多少订单计算出来的（目的是为了判断出稀疏数据的情况）
-- arrivetime——>taketime

##-- 2016.9.19 创建ETL
##-- 2016.9.21 第一版修改（它是基于多少订单计算出来的）

##Description##
##-- 本ETL用来产生针对每个菜品的出餐时间的预估

##TaskInfo##
creator = 'chengyuanqi@meituan.com'

source = {
    'db': META['hmart_peisongpa'],
}

stream = {
    'format': '',
}

target = {
    'db': META['hmart_peisongpa'],
    'table': 'cook_time_per_food',
}

##Extract##
##-- Extract节点, 这里填写一个能在source.db上执行的sql


##Preload##
#if $isCLEANUP
  drop table if exists `$target.table`
#end if


##Load##
##-- Load节点, (可以留空)
SET hive.exec.parallel=true;

#if  $isRELOAD

    SET mapred.reduce.tasks=490;

#end if
SET mapred.reduce.tasks=490;

INSERT overwrite TABLE `$target.table` partition(dt=$now.datekey)
select a.food_id as food_id,avg(a.aver_in_order) as cook_time,a.poi_id as poi_id,count(*) as order_count
from
(
	select first.wm_food_id as food_id,(second.taketime - second.ordertime)/first.food_count as aver_in_order,second.platform_poi_id as poi_id
		from
		(
			select * 
			from
			mart_peisongpa.bm_delivery_wm_order_detail as order_detail
			where dt>=$now.delta(7).datekey and dt<$now.datekey and wm_food_id is not null and food_count is not null
		)   as first
		join 
		(	select *
			from
			mart_peisongpa.fact_waybill_track_time as track_time
			where dt>=$now.delta(7).datekey and dt<$now.datekey and taketime is not null and ordertime is not null and platform_poi_id is not null
		)as second
		on 
		first.bm_waybill_id=second.bm_waybill_id
)as a
group by a.food_id,a.poi_id

##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `food_id`         BIGINT      COMMENT '菜品ID',
  `cook_time`       BIGINT      COMMENT '出餐时间',
  `poi_id`          BIGINT      COMMENT '商家ID',
  `order_count`     INT         COMMENT '该时间是基于X个历史订单计算出来的'
)COMMENT '用于进行菜品出餐时间预估的表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC








-- 第三个版本
-- 20160926 更新新的ETL
-- 新的SQL语句，将出餐时间进行了粗分类，基本上分成了0，1，2，3，4，5六类
##Description##
##-- 本ETL用来产生针对每个菜品的出餐时间的预估

##TaskInfo##
creator = 'chengyuanqi@meituan.com'

source = {
    'db': META['hmart_peisongpa'],
}

stream = {
    'format': '',
}

target = {
    'db': META['hmart_peisongpa'],
    'table': 'cook_time_per_food',
}

##Extract##
##-- Extract节点, 这里填写一个能在source.db上执行的sql


##Preload##
#if $isCLEANUP
  drop table if exists `$target.table`
#end if


##Load##
##-- Load节点, (可以留空)
SET hive.exec.parallel=true;

#if  $isRELOAD

    SET mapred.reduce.tasks=490;

#end if
SET mapred.reduce.tasks=490;

INSERT overwrite TABLE `$target.table` partition(dt=$now.datekey)
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
				where dt>=$now.delta(7).datekey and dt<$now.datekey and wm_food_id is not null and food_count is not null
			)   as first
			join 
			(	select *
				from
				mart_peisongpa.fact_waybill_track_time as track_time
				where dt>=$now.delta(7).datekey and dt<$now.datekey and taketime is not null and ordertime is not null and platform_poi_id is not null
			)as second
			on 
			first.bm_waybill_id=second.bm_waybill_id
	)as a
	group by a.food_id,a.poi_id
	) as aa
	)as aaa
 )as t
ORDER BY t.food_id

##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `food_id`         BIGINT      COMMENT '菜品ID',
  `time_level`		INT  		COMMENT '按照时间长短的分类',
  `cook_time`       BIGINT      COMMENT '出餐时间',
  `poi_id`          BIGINT      COMMENT '商家ID'
) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC










-- 第四个版本
-- 20161008 更新新的ETL
-- 新的SQL语句，将出餐时间进行了粗分类，基本上分成了0，1，2，3，4，5六类
##Description##
##-- 本ETL用来产生针对每个菜品的出餐时间的预估，输出有估计的出餐时间和按照出餐时间划分的time_level

##TaskInfo##
creator = 'chengyuanqi@meituan.com'

source = {
    'db': META['hmart_peisongpa'],
}

stream = {
    'format': '',
}

target = {
    'db': META['hmart_peisongpa'],
    'table': 'bm_food_special_cook_time_per_food',
}

##Extract##
##-- Extract节点, 这里填写一个能在source.db上执行的sql


##Preload##
#if $isCLEANUP
  drop table if exists `$target.table`
#end if


##Load##
##-- Load节点, (可以留空)
SET hive.exec.parallel=true;

#if  $isRELOAD

    SET mapred.reduce.tasks=490;

#end if
SET mapred.reduce.tasks=490;

INSERT overwrite TABLE `$target.table` partition(dt=$now.datekey)
select 
aaaa.food_id as food_id,
aaaa.food_name as food_name,
aaaa.time_level as time_level,
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
			select a.bm_waybill_id as bm_waybill_id,a.food_id as food_id,a.food_name as food_name,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count,(a.taketime-a.ordertime)/count(*) over(partition by a.bm_waybill_id) as aver_in_order
			from
			(
				select first.bm_waybill_id as bm_waybill_id,first.ordertime as ordertime,first.taketime as taketime,first.platform_poi_id as poi_id,order_detail.wm_food_id as food_id,order_detail.food_name as food_name,order_detail.count as count
				from
				(select bm_waybill_id,ordertime,taketime,platform_poi_id
				from mart_peisongpa.fact_waybill_track_time as track_time
				where dt>=$now.delta(30).datekey and dt<$now.datekey and bm_waybill_id is not null and ordertime is not null and taketime is not null) as first
				join
				(select id,platform_origin_order_id
				from mart_peisong.fact_waybill_pkg_info as pkg_info 
				where dt>=$now.delta(30).datekey and dt<$now.datekey) as second
				on first.bm_waybill_id=second.id
				join
				(
				select wm_order_id,wm_food_id,food_name,count
				from origindb.waimai__wm_order_detail
				)as order_detail
				on second.platform_origin_order_id = order_detail.wm_order_id
			)as a
		)as aa
		group by aa.food_id,aa.food_name,aa.poi_id
	) as aaa
)as aaaa

##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `food_id`         BIGINT      COMMENT '菜品ID',
  `food_name`		string   	COMMENT '菜品名称',
  `time_level`		INT 		COMMENT '时间聚类结果(分类号)',
  `cook_time`       BIGINT      COMMENT '出餐时间',
  `poi_id`          BIGINT      COMMENT '商家ID',
  `order_count`		INT 		COMMENT '该出餐时间根据的历史订单的个数'
) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
















-- -- 新的ETL
-- -- 归一化时间
-- -- 第四个版本
-- -- 20161008 更新新的ETL
-- -- 新的SQL语句，将出餐时间进行了粗分类，基本上分成了0，1，2，3，4，5六类
-- ##Description##
-- ##-- 本ETL用来产生针对每个菜品的出餐时间的预估

-- ##TaskInfo##
-- creator = 'chengyuanqi@meituan.com'

-- source = {
--     'db': META['hmart_peisongpa'],
-- }

-- stream = {
--     'format': '',
-- }

-- target = {
--     'db': META['hmart_peisongpa'],
--     'table': 'cook_time_per_food',
-- }

-- ##Extract##
-- ##-- Extract节点, 这里填写一个能在source.db上执行的sql


-- ##Preload##
-- #if $isCLEANUP
--   drop table if exists `$target.table`
-- #end if


-- ##Load##
-- ##-- Load节点, (可以留空)
-- SET hive.exec.parallel=true;

-- #if  $isRELOAD

--     SET mapred.reduce.tasks=490;

-- #end if
-- SET mapred.reduce.tasks=490;

-- INSERT overwrite TABLE `$target.table` partition(dt=$now.datekey)
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
-- 					(select *
-- 					from mart_peisongpa.fact_waybill_track_time as track_time
-- 					where dt>=$now.delta(7).datekey and dt<$now.datekey and bm_waybill_id is not null and ordertime is not null and taketime is not null) 
-- 					as first
-- 					join
-- 					(select *
-- 					from mart_peisong.fact_waybill_pkg_info as pkg_info 
-- 					where dt>=$now.delta(7).datekey and dt<$now.datekey)
-- 					as second
-- 					on first.bm_waybill_id=second.id
-- 					join
-- 					origindb.waimai__wm_order_detail as order_detail
-- 					on second.platform_origin_order_id = order_detail.wm_order_id
-- 					join
-- 					mart_waimaigrowth.food_general2origin as general2origin
-- 					on order_detail.wm_food_id =  general2origin.wm_food_id
-- 			)as a
-- 		)as aa
-- 		group by aa.rk_grp,aa.poi_id
-- 	) as aaa
-- )as aaaa

-- ##TargetDDL##
-- ##-- 目标表表结构
-- CREATE TABLE IF NOT EXISTS `$target.table`
-- (
--   `rk_grp`          BIGINT      COMMENT '归一化后的菜品ID',
--   `cook_time`       BIGINT      COMMENT '出餐时间',
--    `time_level`	    INT 		COMMENT '时间聚类结果(分类号)',
--   `poi_id`          BIGINT      COMMENT '商家ID'

-- ) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
-- PARTITIONED BY (dt string)
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
-- STORED AS ORC




-- -- 性能太差啦，这个代码去掉了poi_id字段，然后合并了相同的部分

-- ##Description##
-- ##-- 本ETL用来产生针对每个菜品的出餐时间的预估

-- ##TaskInfo##
-- creator = 'chengyuanqi@meituan.com'

-- source = {
--     'db': META['hmart_peisongpa'],
-- }

-- stream = {
--     'format': '',
-- }

-- target = {
--     'db': META['hmart_peisongpa'],
--     'table': 'cook_time_per_food',
-- }

-- ##Extract##
-- ##-- Extract节点, 这里填写一个能在source.db上执行的sql


-- ##Preload##
-- #if $isCLEANUP
--   drop table if exists `$target.table`
-- #end if


-- ##Load##
-- ##-- Load节点, (可以留空)
-- SET hive.exec.parallel=true;

-- #if  $isRELOAD

-- SET mapred.reduce.tasks=490;

-- #end if
-- SET mapred.reduce.tasks=490;

-- INSERT overwrite TABLE `$target.table` partition(dt=$now.datekey)
-- select distinct
-- aaaaa.rk_grp as rk_grp,
-- aaaaa.cook_time as cook_time,
-- aaaaa.time_level as time_level
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
-- 					where dt>=$now.delta(7).datekey and dt<$now.datekey and bm_waybill_id is not null and ordertime is not null and taketime is not null) 
-- 					as first
-- 					join
-- 					(select id,platform_origin_order_id
-- 					from mart_peisong.fact_waybill_pkg_info as pkg_info 
-- 					where dt>=$now.delta(7).datekey and dt<$now.datekey)
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

-- ##TargetDDL##
-- ##-- 目标表表结构
-- CREATE TABLE IF NOT EXISTS `$target.table`
-- (
--   `rk_grp`          BIGINT      COMMENT '归一化后的菜品ID',
--   `cook_time`       BIGINT      COMMENT '出餐时间',
--    `time_level`	    INT 		COMMENT '时间聚类结果(分类号)'

-- ) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
-- PARTITIONED BY (dt string)
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
-- STORED AS ORC





