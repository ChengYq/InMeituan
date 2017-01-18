SELECT
tmp2.abnormal_reason as abnormal_reason,
AVG(tmp2.taketime) as avg_take_time,
AVG(tmp2.send_duration) as avg_send_duration,
AVG(tmp2.deliver_duration) as avg_deliver_duration,
AVG(tmp2.abs_error) as avg_abs_error

FROM
(	
	SELECT 
	tmp.bm_waybill_id AS bm_waybill_id,
	tmp.take_duration AS taketime,
	tmp.send_duration AS send_duration,
	tmp.deliver_duration AS deliver_duration,
	tmp.abs_error AS abs_error,
	CASE
		WHEN tmp.abnormal_reason LIKE "%餐%" THEN "出餐慢"
		WHEN tmp.abnormal_reason LIKE "%地址%" THEN "地址问题"
		WHEN tmp.abnormal_reason LIKE "%电话%" THEN "电话联系不上"
		WHEN tmp.abnormal_reason IS NULL THEN "正常订单"
		ELSE "其它异常"	
	END AS abnormal_reason
	FROM
	(
		SELECT 
		time_table.bm_waybill_id AS bm_waybill_id,
		abnormal.abnormal_reason AS abnormal_reason,
		(time_table.taketime - time_table.ordertime) AS take_duration,
		(time_table.finished_time-time_table.taketime ) AS send_duration,
		(time_table.finished_time-time_table.ordertime) AS deliver_duration,
		abs(time_table.delivered_time- time_table.finished_time) AS abs_error
		FROM
		(
			SELECT 
			bm_waybill_id,
			taketime,
			ordertime,
			delivered_time,
			finished_time
			FROM mart_peisongpa.fact_waybill_track_time
			WHERE dt>='20161108' 
			AND dt<='20161114'
			AND taketime IS NOT NULL
			AND ordertime IS NOT NULL
			AND delivered_time IS NOT NULL
			AND finished_time != 0
			AND delivered_time !=0
		)AS time_table
		LEFT OUTER JOIN 
		(
			SELECT 
			waybill_id,
			abnormal_reason
			FROM mart_peisong.fact_rider_report_exception_day
			WHERE dt>='20161108' 
			AND dt<='20161114' 
		)AS abnormal
		ON
		abnormal.waybill_id = time_table.bm_waybill_id
	)AS tmp
)AS tmp2

GROUP BY tmp2.abnormal_reason





















##Description##
##-- 本ETL用来产生骑手上报的异常原因分类

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
    'table': 'bm_waybill_normalized_abnormal_reason',
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
SELECT 
waybill_id as bm_waybill_id,
abnormal_reason,
CASE
	WHEN tmp.abnormal_reason LIKE "%餐%" THEN "出餐慢"
	WHEN tmp.abnormal_reason LIKE "%地址%" THEN "地址有误"
	WHEN tmp.abnormal_reason LIKE "%电话%" THEN "电话联系不上"
	ELSE "其它异常"	
END AS normalized_reason
FROM mart_peisong.fact_rider_report_exception_day as tmp
WHERE dt=$now.datekey



##TargetDDL##
##-- 目标表表结构
CREATE TABLE IF NOT EXISTS `$target.table`
(
  `bm_waybill_id`       BIGINT         COMMENT '运单ID',
  `abnormal_reason`     STRING          COMMENT '骑手上报的异常原因',
  `normalized_reason` 	STRING 		   COMMENT '异常原因分类：出餐慢；地址有误；电话联系不上；其它异常',

) COMMENT '骑手上报的异常原因分类表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC


