package com.dc.game.shardingspringbootstarter.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.api.config.sharding.strategy.ComplexShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.HintShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.InlineShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.hint.HintShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.complex.ComplexShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.hint.HintShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.inline.InlineShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.standard.StandardShardingStrategy;
import org.springframework.util.Assert;

import java.lang.reflect.Constructor;

/**
 * 类的功能描述
 *  构建表规则
 * @author 自己姓名
 * @date 2021/4/12 19:54
 */
@Slf4j
public class TableRuleConfigurationFactionBuilder {


  public static ShardingStrategy buildShardingStrategyConfiguration(String classNameShardingStrategyConfigurationParam,
                                                                    String classNameShardingAlgorithmParam,
                                                                    String shardingColumn,
                                                                    String algorithmExpression)  {
      try {
          //  获取配置分表策略类对象
          Class classNameShardingStrategy = Class.forName(classNameShardingStrategyConfigurationParam);
          // 获取算法类对象
          Class classNameShardingAlgorithm = Class.forName(classNameShardingAlgorithmParam);

          if (StandardShardingStrategy.class.isAssignableFrom(classNameShardingStrategy)){

              if (RangeShardingAlgorithm.class.isAssignableFrom(classNameShardingAlgorithm)
                      && PreciseShardingAlgorithm.class.isAssignableFrom(classNameShardingAlgorithm)){
                  Constructor constructor  = StandardShardingStrategyConfiguration.class.getConstructor(String.class,
                          PreciseShardingAlgorithm.class,RangeShardingAlgorithm.class);

                  StandardShardingStrategyConfiguration shardingStrategyConfiguration =  (StandardShardingStrategyConfiguration)constructor.newInstance(shardingColumn,
                          classNameShardingAlgorithm.newInstance(), classNameShardingAlgorithm.newInstance());
                  return new StandardShardingStrategy(shardingStrategyConfiguration);
              }

              if (RangeShardingAlgorithm.class.isAssignableFrom(classNameShardingAlgorithm)){
                  Constructor constructor  = StandardShardingStrategyConfiguration.class.getConstructor(String.class,
                          PreciseShardingAlgorithm.class,RangeShardingAlgorithm.class);

                  StandardShardingStrategyConfiguration shardingStrategyConfiguration=   (StandardShardingStrategyConfiguration)constructor.newInstance(shardingColumn,
                         null, classNameShardingAlgorithm.newInstance());
                  return new StandardShardingStrategy(shardingStrategyConfiguration);
              }

              if (PreciseShardingAlgorithm.class.isAssignableFrom(classNameShardingAlgorithm)){
                  Constructor constructor  = StandardShardingStrategyConfiguration.class.getConstructor(String.class,PreciseShardingAlgorithm.class);

                  StandardShardingStrategyConfiguration  shardingStrategyConfiguration =  (StandardShardingStrategyConfiguration)constructor.newInstance(shardingColumn,
                          classNameShardingAlgorithm.newInstance());
                  return new StandardShardingStrategy(shardingStrategyConfiguration);
              }


          }
          if (ComplexShardingStrategy.class.isAssignableFrom(classNameShardingStrategy)){
              if (ComplexKeysShardingAlgorithm.class.isAssignableFrom(classNameShardingAlgorithm)){
                  Constructor   constructor  = ComplexShardingStrategyConfiguration.class.getConstructor(String.class,ComplexKeysShardingAlgorithm.class);
                  ComplexShardingStrategyConfiguration shardingStrategyConfiguration =  (ComplexShardingStrategyConfiguration)constructor.newInstance(shardingColumn,
                          classNameShardingAlgorithm.newInstance(), classNameShardingAlgorithm.newInstance());
                  return new ComplexShardingStrategy(shardingStrategyConfiguration);
              }
          }

          if (HintShardingStrategy.class.isAssignableFrom(classNameShardingStrategy)){
              if (HintShardingAlgorithm.class.isAssignableFrom(classNameShardingAlgorithm)){
                  Constructor constructor  = HintShardingStrategyConfiguration.class.getConstructor(String.class,HintShardingStrategyConfiguration.class);
                  HintShardingStrategyConfiguration shardingStrategyConfiguration =  (HintShardingStrategyConfiguration)constructor.newInstance(shardingColumn,
                          classNameShardingAlgorithm.newInstance(), classNameShardingAlgorithm.newInstance());
                  return new HintShardingStrategy(shardingStrategyConfiguration);
              }
          }
          if (InlineShardingStrategy.class.isAssignableFrom(classNameShardingStrategy)){
              Constructor constructor  = InlineShardingStrategyConfiguration.class.getConstructor(String.class,String.class);
              InlineShardingStrategyConfiguration shardingStrategyConfiguration =  (InlineShardingStrategyConfiguration)constructor.newInstance(shardingColumn,
                      algorithmExpression);
              return new InlineShardingStrategy(shardingStrategyConfiguration);
          }

          Assert.isTrue(false,"不支持这个分表类型");
         } catch (Exception e) {
          e.printStackTrace();
          log.error("构建分表策略异常",e);
          Assert.isTrue(false,"构建分表策略异常");
      }
       return null;
  }


}
