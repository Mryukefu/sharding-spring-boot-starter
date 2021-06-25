package com.dc.game.shardingspringbootstarter.entry.enumkey;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * 分表表达式配置
 */
public enum TableRuleConfigurationEnum {

    ADMIN_DB_TABLE_USER("new_dc_sdk_master", "dc_user","user_id",
            "user_id","_${0..1}");

    // 主从数据库数据库名称yml 配置一样
    private String msName;

    // 数据库名称yml 配置一样
    private String dbName;
    // 逻辑表名称
    private String logicTable;
    // 表主键
    private String generatorColumnName;
    // 分片键
    private String shardingColumn;
    // 分表表达式
    private String expression;
    //  分表算法表达式 // 主要用户行表达式策略InlineShardingStrategyConfiguration
    private String algorithmExpression;
    //  分表规则配置策略 目前有四种
   //  StandardShardingStrategyConfiguration,
    // ComplexShardingStrategyConfiguration
    // HintShardingStrategyConfiguration
    // InlineShardingStrategyConfiguration
    private Class  classNameShardingStrategyConfiguration ;//= StandardShardingStrategyConfiguration.class;

    //  分表算法实现类
    // 主要实现RangeShardingAlgorithm，
    // PreciseShardingAlgorithm
    // ComplexKeysShardingAlgorithm
    private Class classNameShardingAlgorithm ;//= CreateFieldShardingAlgorithm.class;

    public String getDbName() {
        return dbName;
    }


    public String getLogicTable() {
        return logicTable;
    }


    public String getGeneratorColumnName() {
        return generatorColumnName;
    }


    public String getShardingColumn() {
        return shardingColumn;
    }


    public String getExpression() {
        return expression;
    }


    public Class getClassNameShardingStrategyConfiguration(){
        return classNameShardingStrategyConfiguration;
    }

    public Class getClassNameShardingAlgorithm(){
        return classNameShardingAlgorithm;
    }


    public String getAlgorithmExpression(){
        return algorithmExpression;
    }

    TableRuleConfigurationEnum(String dbName, String logicTable, String generatorColumnName,
                               String shardingColumn, String expression){
        this.dbName = dbName;
        this.logicTable = logicTable;
        this.generatorColumnName = generatorColumnName;
        this.shardingColumn = shardingColumn;
        this.expression = expression;
    }

    TableRuleConfigurationEnum(String dbName, String logicTable, String generatorColumnName,
                               String shardingColumn, String expression,String algorithmExpression){
        this.dbName = dbName;
        this.logicTable = logicTable;
        this.generatorColumnName = generatorColumnName;
        this.shardingColumn = shardingColumn;
        this.expression = expression;
        this.algorithmExpression = algorithmExpression;
    }


    TableRuleConfigurationEnum(String dbName, String logicTable, String generatorColumnName,
                               String shardingColumn, String expression,
                               Class classNameShardingAlgorithm,Class classNameShardingStrategyConfiguration){
        this.dbName = dbName;
        this.logicTable = logicTable;
        this.generatorColumnName = generatorColumnName;
        this.shardingColumn = shardingColumn;
        this.expression = expression;
        this.classNameShardingAlgorithm = classNameShardingAlgorithm;
        this.classNameShardingStrategyConfiguration = classNameShardingStrategyConfiguration;


    }



    public static List<TableRuleConfigurationEnum> TableRuleConfigurationEnum(String dbName) {
        TableRuleConfigurationEnum[] values = TableRuleConfigurationEnum.values();
        return Arrays.stream(values)
                .filter(t1 -> t1.getDbName().equals(dbName))
                .collect(Collectors.toList());

    }

}
