package com.dc.game.shardingspringbootstarter.algorithm;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collection;


/**
 * 按天分片规则
 * @author wuhongchao
 * @since 2020/11/19
 *
 */
public class CreateTimeMonthShardingAlgorithm implements PreciseShardingAlgorithm<Integer> {

	@Override
	public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Integer> shardingValue) {
		Integer value = shardingValue.getValue();
		String logicTableName = shardingValue.getLogicTableName();
		LocalDate localDate = Instant.ofEpochMilli(value*1000L).atZone(ZoneOffset.ofHours(0)).toLocalDate();
		return logicTableName +"_" + localDate.getYear()+"_"+localDate.getMonthValue();
	}


}
