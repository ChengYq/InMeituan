
##Description##
##-- 本ETL用来计算变量体系里边“菜品数量”这个特征，和亚统、春苗一起合作

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
    'table': 'food_count',
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
-- 第四个版本
-- 20161008 更新新的ETL
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
    'table': 'all_food_count',
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
select a.bm_waybill_id as bm_waybill_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count
from
(
	select first.bm_waybill_id as bm_waybill_id,order_detail.count as count
	from
	(select bm_waybill_id
	from mart_peisongpa.fact_waybill_track_time 
	as track_time
	where dt=$now.datekey
	and bm_waybill_id is not null
) 
	as first
	join
	(select id,platform_origin_order_id
	from mart_peisong.fact_waybill_pkg_info
	as pkg_info 
	where dt=$now.datekey) 
	as second
	on first.bm_waybill_id=second.id
	join
	origindb.waimai__wm_order_detail 
	as order_detail
	on second.platform_origin_order_id = order_detail.wm_order_id
)as a

##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `bm_waybill_id`   BIGINT      COMMENT '运单ID',
  `kind_count`		INT      	COMMENT '菜品种类数量',
  `food_count`		INT 		COMMENT '菜品数量'
) COMMENT '用于变量体系（运单菜品数量)'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC











------------增加了两个变量，sum_time_level,以及sum_cook_time。
------------

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

aaaa.time_level as time_level,
avg(aaaa.cook_time) over (partition by aaaa.time_level) as cook_time,
aaaa.kind_count as kind_count,
aaaa.food_count as food_count
from
(
	select  aaa.food_id as food_id,
	aaa.cook_time as cook_time,
	case 
		when aaa.cook_time <= 50 then 0
		when aaa.cook_time >= 50 and aaa.cook_time< 350 then 1
		when aaa.cook_time >= 350 and aaa.cook_time< 850 then 2
		when aaa.cook_time >= 850 and aaa.cook_time< 1200 then 3
		when aaa.cook_time >= 1200 and aaa.cook_time< 2000 then 4
		else 5
	end as time_level,
	aaa.kind_count as kind_count,
	aaa.food_count as food_count
	from
	(
		select 
		aa.food_id as food_id,
		avg(aver_in_order) as cook_time,
		aa.kind_count as kind_count,
		aa.food_count as food_count
		from
		(
			select 
			a.bm_waybill_id as bm_waybill_id,
			a.wm_order_id as wm_order_id,
			a.food_id as food_id,
			count(*) over(partition by a.bm_waybill_id) as kind_count,
			sum(a.count) over(partition by a.bm_waybill_id) as food_count,
			(a.taketime-a.ordertime)/count(*) over(partition by a.bm_waybill_id) as aver_in_order
			from
			(
				select 
				first.bm_waybill_id as bm_waybill_id,
				first.ordertime as ordertime,
				first.taketime as taketime,
				order_detail.wm_order_id as wm_order_id,
				order_detail.wm_food_id as food_id,
				order_detail.count as count
				from
				(select 
					bm_waybill_id,
					ordertime,
					taketime
				from mart_peisongpa.fact_waybill_track_time as track_time
				where dt>=$now.delta(30).datekey and dt<$now.datekey and bm_waybill_id is not null and ordertime is not null and taketime is not null)
				as first
				join
				(select 
					id,
					platform_origin_order_id
				from mart_peisong.fact_waybill_pkg_info as pkg_info 
				where dt>=$now.delta(30).datekey and dt<$now.datekey) 
				as second
				on first.bm_waybill_id=second.id
				join
				(select 
					wm_order_id,
					wm_food_id,
					count
				 from origindb.waimai__wm_order_detail
				)as order_detail
				on second.platform_origin_order_id = order_detail.wm_order_id
			)as a
		)as aa
		group by aa.food_id,aa.kind_count,aa.food_count
	) as aaa
)as aaaa

##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `food_id`         BIGINT      COMMENT '菜品ID',
  `time_level`		INT 		COMMENT '时间聚类结果(分类号)',
  `cook_time`       BIGINT      COMMENT '出餐时间',
  `kind_count`      INT         COMMENT '菜品种类数量',
  `food_count`      INT         COMMENT '菜品数量'
) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
