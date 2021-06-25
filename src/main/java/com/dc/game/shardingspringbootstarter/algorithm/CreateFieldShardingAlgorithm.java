package com.dc.game.shardingspringbootstarter.algorithm;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

import java.util.Collection;

/**
 * 按照主键分表
 * @author ykf
 * @since 2020/11/19
 *
 */
public class CreateFieldShardingAlgorithm implements PreciseShardingAlgorithm<Long> {

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<Long> shardingValue) {
        Long value = shardingValue.getValue();
        String logicTableName = shardingValue.getLogicTableName();
        return logicTableName+"_"+value%10;
    }


}
