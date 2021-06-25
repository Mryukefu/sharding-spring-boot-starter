package com.dc.game.shardingspringbootstarter.entry.po;

import lombok.Data;

/**
 * 类的功能描述
 *  分片表对应实体类
 * @author 自己姓名
 * @date 2021/4/13 17:21
 */
@Data
public class DbDataNodes {

    /** 主从数据库名称 **/
   // private String masterSlaveDateSourceName;
    /** 从数据库名称, 逗号拼接 **/
   // private String slaveDateSourceNames;
    /** 数据库名称 **/
   //  private String masterDataSourceName;
    /** 数据库名称 **/
    private String dataSourceName;
    /** 逻辑表名称 **/
    private String logicTableName;
    /** 逻辑表名称 **/
    private String createTableTemplate;
    /** 建表数量 **/
    private Integer createNum;
    /** 1：按照年 2：按季度3:按照月份 4:按照主键取模 **/
    private Integer type;
    /** 逻辑表名称 **/
    private String expression;
    /** 算法表达式 **/
    private String algorithmExpression;
    /** 分表策略 **/
    private String classNameShardingStrategy;
    /** 分表算法全限定类名称 **/
    private String classNameShardingAlgorithm;
    /** 主键 **/
    private String generatorColumnName;
    /** 分表键 **/
    private String shardingColumns;
    /** 状态 1 可用 2 删除 **/
    private Integer state;
    /** 创建时间 **/
    private Integer createTime;

    /** 实际上得表 **/
    private String authenticTableName;

    /** 实际上得表 **/
    private String keyGenerator;

    /** 分组表 **/
    private String tableGroups;



}
