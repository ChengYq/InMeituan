-- 最终版的出餐时间预估：
    -- 创建表mart_peisongpa.bm_food_special_cook_time_per_food，其字段为：food_id, food_name ,time_level, cook_time, poi_id ,order_count
    -- 创建表mart_peisongpa.bm_poi_special_cook_time_per_poi , 其字段为 poi_id,avg_time_level, avg_cook_time，从a表中取数字，
    -- 计算常数：从b表中取avg_time_level, avg_cook_time得再次平均。
    -- 创建表mart_peisongpa.bm_waybill_special_time_and_count ，其字段为： bm_waybill_id, order_id ,food_count ,time_level ,cook_time，其中，order_id能够和烽火台中的相对应。



-- 创建表mart_peisongpa.bm_food_special_cook_time_per_food
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

















-- mart_peisongpa.bm_poi_special_cook_time_per_poi , 其字段为 poi_id,avg_time_level, avg_cook_time，avg_all_cook_time,avg_all_time_level 从a表中取数字
##Description##
##-- 本ETL用来产生针对每个菜品的出餐时间的预估的时间兜底，其中，avg_cook_time是每个poi的平均每个菜品的出餐时间 

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
    'table': 'bm_poi_special_cook_time_per_poi',
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
SELECT aa.poi_id,aa.avg_time_level,aa.avg_cook_time,avg(avg_cook_time)over(partition by aa.dt ) as avg_all_cook_time,avg(avg_time_level)over(partition by aa.dt ) as avg_all_time_level
from
(
select a.poi_id as poi_id,avg(a.time_level) as avg_time_level,avg(a.cook_time) as avg_cook_time,a.dt as dt
from
(
select *
from mart_peisongpa.bm_food_special_cook_time_per_food
where dt=$now.datekey
)as a
group by a.poi_id,a.dt
) as aa


##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `poi_id`          BIGINT      COMMENT '商家ID',
  `avg_time_level`  DOUBLE         COMMENT '商家平均每个菜品的time_level',
  `avg_cook_time`	DOUBLE 		COMMENT '商家平均每个菜品的cook_time',
  `avg_all_cook_time`   DOUBLE   COMMENT '全部的cook_time的平均值',
  `avg_all_time_level`  DOUBLE   COMMENT '全部的time_level的平均值'
) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC













##Description##
##-- 本ETL用来产生最后版本的菜品出餐时间预估

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
    'table': 'bm_food_special_count_and_cook_time',
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
ttttt.bm_waybill_id as bm_waybill_id,
ttttt.kind_count as kind_count,
ttttt.food_count as food_count,
avg(ttttt.time_level) as time_level,
avg(ttttt.cook_time) as cook_time
from (
	select 
	tttt.bm_waybill_id as bm_waybill_id,
	tttt.t0_poi_id as t0_poi_id,
	tttt.t1_poi_id as t1_poi_id,
	tttt.t0_food_id as t0_food_id,
	tttt.t1_food_id as t1_food_id,
	tttt.kind_count as kind_count,
	tttt.food_count as food_count,
	case
		when tttt.t1_food_id is not null then tttt.time_level
		when tttt.t1_food_id is null and tttt.ttt_poi_id is not null then tttt.avg_time_level
		else  1.4858
	end as time_level,
	case
		when tttt.t1_food_id is not null then tttt.cook_time
		when tttt.t1_food_id is null and tttt.ttt_poi_id is not null then tttt.avg_cook_time
		else  371.92492
	end as cook_time
	from
	(

		select t0.bm_waybill_id as bm_waybill_id,t0.food_id as t0_food_id,t1.food_id as t1_food_id,t0.poi_id as t0_poi_id,t1.poi_id as t1_poi_id,t0.kind_count as kind_count,t0.food_count as food_count,ttt.poi_id as ttt_poi_id,t1.time_level as time_level,t1.cook_time as cook_time,ttt.avg_time_level as avg_time_level, ttt.avg_cook_time as avg_cook_time,ttt.avg_all_time_level as avg_all_time_level,ttt.avg_all_cook_time as avg_all_cook_time
		from
		(
			select a.bm_waybill_id as bm_waybill_id,a.food_id as food_id,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count
			from
			(
				select first.bm_waybill_id as bm_waybill_id,first.platform_poi_id as poi_id,order_detail.wm_food_id as food_id,order_detail.count as count
				from
				(select bm_waybill_id,platform_poi_id
				from mart_peisongpa.fact_waybill_track_time as track_time
				where dt=$now.datekey) as first
				join
				(select id,platform_origin_order_id
				from mart_peisong.fact_waybill_pkg_info as pkg_info 
				where dt=$now.datekey) as second
				on first.bm_waybill_id=second.id
				join
				(
				select wm_order_id,wm_food_id,food_name,count
				from origindb.waimai__wm_order_detail
				)as order_detail
				on second.platform_origin_order_id = order_detail.wm_order_id
			)as a
		)as t0
		left outer join
		(select food_id,poi_id,time_level,cook_time,order_count
		from mart_peisongpa.bm_food_special_cook_time_per_food
		where dt='20161014'     
		order by order_count DESC 
		limit 1000000) as t1
		on t0.food_id=t1.food_id 
		left outer join 
		(select 
		poi_id,avg_time_level,avg_cook_time,avg_all_cook_time,avg_all_time_level
		from mart_peisongpa.bm_poi_special_cook_time_per_poi 
		where dt='20161014') as ttt
		on t0.poi_id=ttt.poi_id
--   282~319已经无误
	)as tttt
)as ttttt
group by ttttt.bm_waybill_id, ttttt.kind_count,ttttt.food_count


##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `bm_waybill_id`       BIGINT         COMMENT '运单ID',
  `kind_count`          BIGINT         COMMENT '菜品种类数',
  `food_count` 	        BIGINT 		   COMMENT '菜品数目',
  `time_level`          DOUBLE         COMMENT '预估出餐时间的分类值',
  `cook_time`           DOUBLE         COMMENT '预估出餐时间'
) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC














middle作为中间值

-- middle 

##Description##
##-- 本ETL用来产生最后版本的菜品出餐时间预估

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
    'table': 'bm_food_special_count_and_cook_time_middle',
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
	tttt.bm_waybill_id as bm_waybill_id,
	tttt.t0_poi_id as t0_poi_id,
	tttt.t1_poi_id as t1_poi_id,
	tttt.t0_food_id as t0_food_id,
	tttt.t1_food_id as t1_food_id,
	tttt.kind_count as kind_count,
	tttt.food_count as food_count,
	case
		when tttt.t1_food_id is not null then tttt.time_level
		when tttt.t1_food_id is null and tttt.ttt_poi_id is not null then tttt.avg_time_level
		else  1.4858
	end as time_level,
	case
		when tttt.t1_food_id is not null then tttt.cook_time
		when tttt.t1_food_id is null and tttt.ttt_poi_id is not null then tttt.avg_cook_time
		else  371.92492
	end as cook_time
	from
	(

		select t0.bm_waybill_id as bm_waybill_id,t0.food_id as t0_food_id,t1.food_id as t1_food_id,t0.poi_id as t0_poi_id,t1.poi_id as t1_poi_id,t0.kind_count as kind_count,t0.food_count as food_count,ttt.poi_id as ttt_poi_id,t1.time_level as time_level,t1.cook_time as cook_time,ttt.avg_time_level as avg_time_level, ttt.avg_cook_time as avg_cook_time,ttt.avg_all_time_level as avg_all_time_level,ttt.avg_all_cook_time as avg_all_cook_time
		from
		(
			select a.bm_waybill_id as bm_waybill_id,a.food_id as food_id,a.poi_id as poi_id,count(*) over(partition by a.bm_waybill_id) as kind_count,sum(a.count) over(partition by a.bm_waybill_id) as food_count
			from
			(
				select first.bm_waybill_id as bm_waybill_id,first.platform_poi_id as poi_id,order_detail.wm_food_id as food_id,order_detail.count as count
				from
				(select bm_waybill_id,platform_poi_id
				from mart_peisongpa.fact_waybill_track_time as track_time
				where dt=$now.datekey) as first
				join
				(select id,platform_origin_order_id
				from mart_peisong.fact_waybill_pkg_info as pkg_info 
				where dt=$now.datekey) as second
				on first.bm_waybill_id=second.id
				join
				(
				select wm_order_id,wm_food_id,food_name,count
				from origindb.waimai__wm_order_detail
				)as order_detail
				on second.platform_origin_order_id = order_detail.wm_order_id
			)as a
		)as t0
		left outer join
		(select food_id,poi_id,time_level,cook_time,order_count
		from mart_peisongpa.bm_food_special_cook_time_per_food
		where dt='20161014'     
		order by order_count DESC 
		limit 1000000) as t1
		on t0.food_id=t1.food_id 
		left outer join 
		(select 
		poi_id,avg_time_level,avg_cook_time,avg_all_cook_time,avg_all_time_level
		from mart_peisongpa.bm_poi_special_cook_time_per_poi 
		where dt='20161014') as ttt
		on t0.poi_id=ttt.poi_id
--   282~319已经无误
	)as tttt



##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `bm_waybill_id`       BIGINT         COMMENT '运单ID',
  `t0_poi_id`          BIGINT         COMMENT 't0_poi_id',
  `t1_poi_id` 	        BIGINT 		   COMMENT 't1_poi_id',
  `t0_food_id`          DOUBLE         COMMENT 't0_food_id',
  `t1_food_id`           DOUBLE         COMMENT 't1_food_id',
  `kind_count`          BIGINT          COMMENT '种类数',
  `food_count`			BIGINT			COMMENT '菜品数'
) COMMENT '为了解决一个很复杂的bug，不得已产生的中间表。'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC












!!!!!!!!!!!!!!!!!!!!!
同时加上下边这个：
!!!!!!!!!!!!!!!!!!!!!







##Description##
##-- 本ETL用来产生最后版本的菜品出餐时间预估

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
    'table': 'bm_food_special_count_and_cook_time',
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
mid.bm_waybill_id as bm_waybill_id,
mid.kind_count as kind_count,
mid.food_count as food_count,
avg(mid.time_level) as time_level,
avg(mid.cook_time) as cook_time
from  
(	
	select 
	a.bm_waybill_id as bm_waybill_id,
	a.kind_count as kind_count,
	a.food_count as food_count,
	a.time_level as time_level,
	a.cook_time as cook_time
	from
	mart_peisongpa.bm_food_special_count_and_cook_time_middle as a 
	where a.dt=$now.datekey
)
as mid
group by mid.bm_waybill_id, mid.kind_count,mid.food_count


##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `bm_waybill_id`       BIGINT         COMMENT '运单ID',
  `kind_count`          BIGINT         COMMENT '菜品种类数',
  `food_count` 	        BIGINT 		   COMMENT '菜品数目',
  `time_level`          DOUBLE         COMMENT '预估出餐时间的分类值',
  `cook_time`           DOUBLE         COMMENT '预估出餐时间'
) COMMENT '用于进行菜品出餐时间预估的表（总体聚类)'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC








