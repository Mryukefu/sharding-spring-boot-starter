package com.dc.game.shardingspringbootstarter.wrap;

import org.apache.shardingsphere.core.strategy.keygen.SnowflakeShardingKeyGenerator;
import org.apache.shardingsphere.core.strategy.keygen.UUIDShardingKeyGenerator;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WrapKeyGenerator {

    private final  SnowflakeShardingKeyGenerator snowflakeShardingKeyGenerator = new SnowflakeShardingKeyGenerator();

    private final  UUIDShardingKeyGenerator uuidShardingKeyGenerator = new UUIDShardingKeyGenerator();


    public  String keyGeneratorUuid() {
        return (String)uuidShardingKeyGenerator.generateKey();

    }

    public  Integer keyGenerator() {
        return (Integer) snowflakeShardingKeyGenerator.generateKey();
    }


    public  Long keyGeneratorLong() {
        return (Long) snowflakeShardingKeyGenerator.generateKey();
    }


}
