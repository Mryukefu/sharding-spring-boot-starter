package com.dc.game.shardingspringbootstarter.event;

import com.dc.game.shardingspringbootstarter.config.DsProps;
import com.dc.game.shardingspringbootstarter.config.TableRuleConfigurationFactionBuilder;
import com.dc.game.shardingspringbootstarter.entry.po.DbDataNodes;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.core.rule.BindingTableRule;
import org.apache.shardingsphere.core.rule.MasterSlaveRule;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.ShardingDataSource;
import org.apache.shardingsphere.underlying.common.rule.DataNode;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import tk.mybatis.mapper.util.StringUtil;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 刷新表结构
 *
 * @author ykf
 * @since 2021/4/14
 */
@Component
@EnableConfigurationProperties({DsProps.class})
@Slf4j
public class ActualTableRuleRefreshFromBbEvent {

    public static final String DB_TABLE = ".";

    private ShardingDataSource shardingDataSource;

    @Autowired
    private DsProps dsProps;


    /**
     *  获取主从的关系
     * @return
     */
    public Map<String, List<DsProps.DsProp>> getMaterRelateSlave() {
        List<DsProps.DsProp> ds = dsProps.getDs();
        Map<String, List<DsProps.DsProp>> materRelateSlave =
                ds.stream().filter(dProp->dProp.getSlaveDs()!=null).
                        collect(Collectors.toMap(DsProps.DsProp::getDcName, DsProps.DsProp::getSlaveDs));
        return materRelateSlave;

    }


    /**
     * 获取所有的主库
     * @return
     */
    public List<String> getMaterNames() {
       return new ArrayList<>(dsProps.getDs().stream().map(DsProps.DsProp::getDcName).collect(Collectors.toSet()));

    }


    public Set<String> getSlaveNames() {
        List<DsProps.DsProp> ds = dsProps.getDs();
        if (ds==null){
            return null;
        }
       return ds.stream().filter(_dsProps->_dsProps.getSlaveDs()!=null).flatMap(_dsProps1->{
           return  _dsProps1.getSlaveDs().stream().map(DsProps.DsProp::getDcName);
                }
        ).collect(Collectors.toSet());


    }


    /**
     * 获取 逻辑库跟 实际数据库的关系
     * @return
     */
    public Map<String,String> getDbName() {
        Map<String, DataSource> dataSourceMap = shardingDataSource.getDataSourceMap();
        Map<String, String> getDbName = dataSourceMap.keySet().stream()
                .collect(Collectors
                        .toMap(Function.identity(), t1 -> {
                            try {
                                return dataSourceMap.get(t1).getConnection().getCatalog();
                            } catch (SQLException throwables) {
                                throwables.printStackTrace();
                            }
                            return null;
                        }));
        return getDbName;

    }

    /**
     *  获取主从关系(逻辑分组库跟逻辑库的关系)
     * @return
     */
    public Map<String,String> getMaterSalveNames() {
        List<DsProps.DsProp> list = dsProps.getDs();
        return
                list.stream()
                        .collect(Collectors.toMap(DsProps.DsProp::getDcName,DsProps.DsProp::getMsName));

    }


    /**m
     * 表节点刷新调度
     *
     * @param
     * @return {@code void}
     * @author ykf
     * @date 2021/4/16 17:16
     */

    public void setDataSource(DataSource dataSource) {
        Assert.notNull(dataSource, "请配置数据源");
        this.shardingDataSource = (ShardingDataSource) dataSource;
    }

    public void actualDataNodesRefresh(ShardingConnection connection) throws Exception {
        ShardingRule shardingRule = shardingDataSource.getRuntimeContext().getRule();
        // 查询数据库配置的分表节点

        List<DbDataNodes> dbDataNodes = queryCreateTable(connection,getMaterNames().get(0));

        //  校验数据库配置
        checkDataBaseConfig(dbDataNodes);

        // 封装实际表(包含数据库节点 loginTableName+后缀)
        List<DbDataNodes> calculateRequiredTables = calculateRequiredTableNames(dbDataNodes);

        // 创建表规则
        boolean masterSalve = isMasterSalve(shardingRule);
        Collection<TableRule> newTableRules = structureNewTableRule(calculateRequiredTables,masterSalve);

        // 刷新分表规则
        Field actualDataNodesField = ShardingRule.class.getDeclaredField("tableRules");
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(actualDataNodesField, actualDataNodesField.getModifiers() & Modifier.FINAL);
        actualDataNodesField.setAccessible(true);
        actualDataNodesField.set(shardingRule, newTableRules);
        if (newTableRules != null) {
            String flushTable = newTableRules.stream().flatMap(t1 -> {
                List<DataNode> actualDataNodes = t1.getActualDataNodes();
                return actualDataNodes.stream().map(DataNode::getTableName);
            }).collect(Collectors.joining(","));
            log.info("[刷新表规则]：{}", flushTable);
        }

        // 刷新分组规则(连表sql)
        Map<String, List<TableRule>> map = newTableRules.stream().collect(Collectors.groupingBy(TableRule::getLogicTable));
        Set<String> groupSet = dbDataNodes.stream().filter(t1 -> StringUtils.hasText(t1.getTableGroups()))
                .map(DbDataNodes::getTableGroups).collect(Collectors.toSet());
        if (groupSet != null && groupSet.size() > 0) {
            List<BindingTableRule> bindingTableRules = groupSet.stream().map(_binds -> {
                String[] binds = _binds.split(",");
                List<TableRule> tableRules = Arrays.asList(binds).stream().flatMap(t1 -> {
                    return map.get(t1).stream();
                }).collect(Collectors.toList());
                return new BindingTableRule(tableRules);
            }).collect(Collectors.toList());

            Field bindingTableRulesNodesField = ShardingRule.class.getDeclaredField("bindingTableRules");
            modifiersField.setAccessible(true);
            modifiersField.setInt(bindingTableRulesNodesField, bindingTableRulesNodesField.getModifiers() & Modifier.FINAL);
            bindingTableRulesNodesField.setAccessible(true);
            bindingTableRulesNodesField.set(shardingRule, bindingTableRules);
        }
    }

    private boolean isMasterSalve(ShardingRule shardingRule) {
        Collection<MasterSlaveRule> masterSlaveRules = shardingRule.getMasterSlaveRules();
        return masterSlaveRules!=null
                &&masterSlaveRules.size()>0
                &&masterSlaveRules.iterator().hasNext()
                &&StringUtils.hasLength(masterSlaveRules.iterator().next().getMasterDataSourceName());
    }


    /**
     * desc 创建实际上的节点，样式 dbName.tableName
     * @param calculateRequiredTables 数据库配置的分表信息
     * @return {@code java.util.Collection<io.shardingsphere.core.rule.TableRule>}
     * @author ykf
     * @date 2021/4/16 17:17
     */
    private Collection<TableRule> structureNewTableRule(List<DbDataNodes> calculateRequiredTables,Boolean masterSalve)
            throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<TableRule> tableRules = new ArrayList<>();

        // 分库
        List<String> materNames = getMaterNames();

        //  根据逻辑表进行分组
        Map<String, List<DbDataNodes>> map = calculateRequiredTables.stream()
                .collect(Collectors.groupingBy(DbDataNodes::getLogicTableName));
        for (String materName : materNames) {
            for (String logicTableName : map.keySet()) {
                List<DbDataNodes> dbDataNodes = map.get(logicTableName);

                //  数据库规则
                TableRule tableRule = new TableRule("", logicTableName);

                //  设置新节点
                List<DataNode> newDataNodes = new ArrayList<>();

                //  设计数据库索引
                Map<DataNode, Integer> dataNodeIndexMap = Maps.newHashMap();

                //  封装数据
                packageTableRuleInfo(masterSalve, materName, dbDataNodes, newDataNodes, dataNodeIndexMap);

                //  逻辑库对应的表结构
                Map<String, List<String>> dataTableMap =newDataNodes
                        .stream()
                        .collect(Collectors.groupingBy(DataNode::getDataSourceName,Collectors.mapping(DataNode::getTableName,Collectors.toList())));

                //  获取修改权限
                Field modifiersField = Field.class.getDeclaredField("modifiers");
                modifiersField.setAccessible(true);

                // 反射刷新actualDataNodes
                Field actualDatasourceNamesField = TableRule.class.getDeclaredField("actualDatasourceNames");
                actualDatasourceNamesField.setAccessible(true);
                modifiersField.setInt(actualDatasourceNamesField, actualDatasourceNamesField.getModifiers() & Modifier.FINAL);
                actualDatasourceNamesField.set(tableRule, newDataNodes.stream().map(DataNode::getDataSourceName).collect(Collectors.toSet()));

                // 反射刷新actualDataNodes
                Field actualDataNodesField = TableRule.class.getDeclaredField("actualDataNodes");
                actualDataNodesField.setAccessible(true);
                modifiersField.setInt(actualDataNodesField, actualDataNodesField.getModifiers() & Modifier.FINAL);
                actualDataNodesField.set(tableRule, newDataNodes);

                // 反射刷新dataNodeIndexMap
                Field dataNodeIndexMapField = TableRule.class.getDeclaredField("dataNodeIndexMap");
                dataNodeIndexMapField.setAccessible(true);
                modifiersField.setInt(dataNodeIndexMapField, dataNodeIndexMapField.getModifiers() & Modifier.FINAL);
                dataNodeIndexMapField.set(tableRule, dataNodeIndexMap);

                // 反射刷新 tableShardingStrategy
                Field tableShardingStrategyField = TableRule.class.getDeclaredField("tableShardingStrategy");
                tableShardingStrategyField.setAccessible(true);
                modifiersField.setInt(tableShardingStrategyField, tableShardingStrategyField.getModifiers() & Modifier.FINAL);

                //  同一张逻辑表的配置其实是一样的恶
                DbDataNodes firstDataNodes = dbDataNodes.get(0);
                tableShardingStrategyField.set(tableRule, TableRuleConfigurationFactionBuilder
                        .buildShardingStrategyConfiguration(firstDataNodes.getClassNameShardingStrategy(),
                                firstDataNodes.getClassNameShardingAlgorithm(), firstDataNodes.getShardingColumns(), null));

                // 反射刷新  generateKeyColumn
                Field generateKeyColumnField = TableRule.class.getDeclaredField("generateKeyColumn");
                generateKeyColumnField.setAccessible(true);
                modifiersField.setInt(generateKeyColumnField, generateKeyColumnField.getModifiers() & Modifier.FINAL);
                generateKeyColumnField.set(tableRule, firstDataNodes.getGeneratorColumnName());


                // 反射刷新  shardingKeyGenerator
                Field shardingKeyGeneratorField = TableRule.class.getDeclaredField("shardingKeyGenerator");
                shardingKeyGeneratorField.setAccessible(true);
                modifiersField.setInt(shardingKeyGeneratorField, shardingKeyGeneratorField.getModifiers() & Modifier.FINAL);
                shardingKeyGeneratorField.set(tableRule, Class.forName(firstDataNodes.getKeyGenerator()).newInstance());


                // 反射刷新  datasourceToTablesMap
                Field datasourceToTablesMapField = TableRule.class.getDeclaredField("datasourceToTablesMap");
                datasourceToTablesMapField.setAccessible(true);
                modifiersField.setInt(datasourceToTablesMapField, datasourceToTablesMapField.getModifiers() & Modifier.FINAL);
                datasourceToTablesMapField.set(tableRule, dataTableMap);
                tableRules.add(tableRule);
            }

        }

        return tableRules;

    }

    private void packageTableRuleInfo(Boolean masterSalve, String materName,
                                      List<DbDataNodes> dbDataNodes,
                                      List<DataNode> newDataNodes,
                                      Map<DataNode, Integer> dataNodeIndexMap) {
        for (DbDataNodes dbDataNode : dbDataNodes) {
            DataNode dataNode = null;

            // 如果配置了主从
            if (masterSalve) {
                String msName = getMaterSalveNames().get(materName);
                Assert.notNull(msName, "请配置主从数据库");
                dataNode = new DataNode(String.format("%s.%s", msName, dbDataNode.getAuthenticTableName()));

                // 没有配置主从
            } else {
                Assert.notNull(materName, "请配置主数据库");
                dataNode = new DataNode(String.format("%s.%s", materName, dbDataNode.getAuthenticTableName()));
            }
            dataNodeIndexMap.put(dataNode, dbDataNodes.indexOf(dbDataNode));
            newDataNodes.add(dataNode);

        }
    }



    /**
     * 项目启动的时候创建分表信息，且刷新节点
     * @return {@code void}
     * @author ykf
     * @date 2021/4/16 17:18
     */
    public void updateTable() throws Exception {

        // 查询需要的表
        ShardingConnection connection = shardingDataSource.getConnection();

        //  数据库查询表节点(主要是主库) 默认是在第一个主库里面查询表规则
        List<DbDataNodes> list = queryCreateTable(connection,getMaterNames().get(0));

        // 封装实际表(包含数据库节点 loginTableName+分表后缀)
        List<DbDataNodes> calculateRequiredTables = calculateRequiredTableNames(list);

        // 获取 逻辑库跟 实际数据库的关系
        Map<String, String> dbNames = getDbName();
        if (dbNames!=null){
            log.info("[sharding jdbc.....配置的主库有] 分别对应的数据库真实库{}",dbNames.keySet().stream().
                    flatMap(t1->{
                       return Arrays.asList("逻辑库:"+t1,"真实库:"+dbNames.get(t1)).stream();
                    })
                    .collect(Collectors.joining(",")));
        }

        //  获取所有的从库
        Set<String> slaveNames = getSlaveNames();
        if (slaveNames!=null){
            log.info("[sharding jdbc.....配置的从库有]{}",slaveNames.stream().collect(Collectors.joining(",")));
        }

        //  排除从库(建表以主库配置的分表设计为准，主从基于数据库log-bin 基础)
        Map<String, String> logicMasterName = dbNames.entrySet().stream().filter(r -> slaveNames!=null&&!slaveNames.contains(r.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        //  剩下主库配置
        Set<String> existedTableNames = getExistedTableNames(logicMasterName,list
                .stream().map(DbDataNodes::getLogicTableName).collect(Collectors.toSet()), connection);

        // 获取真实的数据源创建表
        for (String logicDataSource : logicMasterName.keySet()) {
            String realDataSource = dbNames.get(logicDataSource);

            //  查询已经存在的表结构
            calculateRequiredTables = calculateRequiredTables.stream().map(dataNodes -> {
                dataNodes.setAuthenticTableName(realDataSource + DB_TABLE + dataNodes.getAuthenticTableName());
                return dataNodes;
            }).filter(t1 -> !existedTableNames.contains(t1.getAuthenticTableName()))
                    .collect(Collectors.toList());

            // 创建表
            createTable(calculateRequiredTables,logicDataSource);
        }
        //  刷新表节点
           actualDataNodesRefresh(connection);
    }


    /**
     * 校验数据库配置
     *
     * @param list
     * @return {@code void}
     * @author ykf
     * @date 2021/6/25 15:22
     */
    private void checkDataBaseConfig(List<DbDataNodes> list) {

        /*Assert.notNull(dsProps, "请配置数据源属性");
        List<DsProps.DsProp> ds = dsProps.getDs();

        //  主从配置
        Set<String> configMasterSalve = ds.stream().map(DsProps.DsProp::getMsName).collect(Collectors.toSet());
        if (configMasterSalve != null && configMasterSalve.size() > 0) {
            Set<String> mastersSalve = list.stream().
                    map(DbDataNodes::getMasterSlaveDateSourceName).collect(Collectors.toSet());
            Assert.isTrue(mastersSalve.containsAll(configMasterSalve) && configMasterSalve.containsAll(mastersSalve), "请检查分表主从名称配置与数据库对应");
        }

        // 主库配置
        Set<String> configMaster = ds.stream().map(DsProps.DsProp::getDcName).collect(Collectors.toSet());
        if (configMaster != null && configMaster.size() > 0) {
            Set<String> masters = list.stream().
                    map(DbDataNodes::getMasterDataSourceName).collect(Collectors.toSet());
            Assert.isTrue(masters.containsAll(configMaster) && configMaster.containsAll(masters), "请检查分表主库名称配置与数据库对应");
        }

       //  从库配置
        Set<String> configSalve = ds.stream().flatMap(t1 -> {
            return t1.getSlaveDs().stream().map(t2 -> t2.getDcName());
        }).collect(Collectors.toSet());
        if (configSalve != null && configSalve.size() > 0) {
            Set<String> salve = list.stream().map(DbDataNodes::getSlaveDateSourceNames)
                    .flatMap(dataNode->{
                      return   Arrays.stream(dataNode.split(","));
                    }).collect(Collectors.toSet());
            Assert.isTrue(salve.containsAll(configSalve) && configSalve.containsAll(salve), "请检查分表从库名称配置与数据库对应");
        }*/



    }

    /**
     * 创建表
     *
     * @param tableNames
     * @return {@code void}
     * @author ykf
     * @date 2021/4/16 16:17
     */
    @Transactional(rollbackFor = Exception.class)
    public void createTable(List<DbDataNodes> tableNames,String logicDataSource) {
        if (tableNames != null && tableNames.size() > 0) {
            try (ShardingConnection connection = shardingDataSource.getConnection(); Statement statement = connection
                    .getConnection(logicDataSource).createStatement()) {
                for (DbDataNodes tableName : tableNames) {
                    statement.executeUpdate(tableName.getCreateTableTemplate());
                }
                log.info("sharding jdbc..... [创建表详情]：逻辑库名称:{}=>新建表：总数:{},表名称{}", logicDataSource,tableNames.size(), tableNames
                        .stream()
                        .map(DbDataNodes::getAuthenticTableName).collect(Collectors.joining(",")));
            } catch (Exception e) {
                log.error("创建表异常", e);
            }
        }
    }


    /**
     * 查询已存在的表（这个已经存在的表是数据库生成的表） 这里需要查询主库为准
     *
     * @param dbName 数据库列表，
     * @param connection
     * @return {@code java.util.List<java.lang.String>}
     * @author ykf
     * @date 2021/4/16 17:19
     */
    private Set<String> getExistedTableNames(Map<String, String> dbName, Set<String> logicTableNames, Connection connection) {
        Set<String> result = new HashSet<>();
        for (String logicDataSource : dbName.keySet()) {
            String realDataSource = dbName.get(logicDataSource);
            try {
                ShardingConnection shardingConnection = (ShardingConnection) connection;
                Connection masterConnection = shardingConnection.getConnection(logicDataSource);
                Assert.notNull(masterConnection, "数据源不存在");
                DatabaseMetaData metaData = masterConnection.getMetaData();
                for (String logicTableName : logicTableNames) {
                    ResultSet tables = metaData.getTables(realDataSource, null,
                            logicTableName + "%", new String[]{"TABLE"});
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        result.add(realDataSource + DB_TABLE + tableName);
                    }
                }
            } catch (Exception e) {
                log.error("[查询表失败]", e);
            }
        }
        return result;
    }


    /**
     * 查询数据库创建的分表信息
     *
     * @param connection
     * @return {@code java.util.List<com.project.common.entity.po.DbDataNodes>}
     * @author ykf
     * @date 2021/4/16 17:21
     */
    private List<DbDataNodes> queryCreateTable(ShardingConnection connection,String masterName) {
        List<DbDataNodes> list = new ArrayList<>();
        //  查询需要创建的表
        try {
                Connection masterConnection = connection.getConnection(masterName);
                Statement statement = masterConnection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM  db_data_nodes  where  state = 1");
                while (resultSet.next()) {
                    DbDataNodes dbDataNodes = new DbDataNodes();
                    /*dbDataNodes.setMasterSlaveDateSourceName(resultSet.getString("master_slave_data_source_name"));
                    dbDataNodes.setMasterDataSourceName(resultSet.getString("master_data_source_name"));
                    dbDataNodes.setSlaveDateSourceNames(resultSet.getString("slave_data_source_name"));*/

                    dbDataNodes.setDataSourceName(resultSet.getString("date_source_name"));
                    dbDataNodes.setLogicTableName(resultSet.getString("logic_table_name"));
                    dbDataNodes.setCreateTableTemplate(resultSet.getString("create_table_template"));
                    dbDataNodes.setCreateNum(resultSet.getInt("create_num"));
                    dbDataNodes.setType(resultSet.getInt("type"));
                    dbDataNodes.setExpression(resultSet.getString("expression"));
                    dbDataNodes.setAlgorithmExpression(resultSet.getString("algorithm_expression"));
                    dbDataNodes.setClassNameShardingStrategy(resultSet.getString("class_name_sharding_strategy"));
                    dbDataNodes.setClassNameShardingAlgorithm(resultSet.getString("class_name_sharding_algorithm"));
                    dbDataNodes.setGeneratorColumnName(resultSet.getString("generator_column_name"));
                    dbDataNodes.setShardingColumns(resultSet.getString("sharding_columns"));
                    dbDataNodes.setKeyGenerator(resultSet.getString("key_generator"));
                    dbDataNodes.setTableGroups(resultSet.getString("table_groups"));
                    dbDataNodes.setState(resultSet.getInt("state"));
                    list.add(dbDataNodes);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }


    /**
     * 计算所需要的表
     *
     * @param
     * @return {@code java.util.List<java.lang.String>}
     * @author ykf
     * @date 2021/4/13 19:47
     */
    private List<DbDataNodes> calculateRequiredTableNames(List<DbDataNodes> list) {
        List<DbDataNodes> result = new ArrayList<>();
        //  这个需要判断是否主从配置 先校验
        //  查询需要创建的表
        for (DbDataNodes dbDataNodes : list) {
            Integer createNum = dbDataNodes.getCreateNum();
            for (Integer num = 0; num < createNum; num++) {
                DbDataNodes newDataNote = new DbDataNodes();
                BeanUtils.copyProperties(dbDataNodes, newDataNote);
                //  按照年
                StringBuilder dataTableName = new StringBuilder(dbDataNodes.getLogicTableName());
                ruleYear(dbDataNodes, dataTableName, num, LocalDate.now());
                // 按季度
                ruleQuarter(dbDataNodes, dataTableName, num, LocalDate.now());
                //  按照月份
                ruleMoth(dbDataNodes, dataTableName, num, LocalDate.now());
                //  按照主键
                ruleClu(dbDataNodes, dataTableName, num);
                newDataNote.setAuthenticTableName(dataTableName.toString());
                newDataNote.setCreateTableTemplate(String.format(newDataNote.getCreateTableTemplate(), dataTableName.toString()));
                result.add(newDataNote);
            }
        }
        return result;
    }


    private void ruleYear(DbDataNodes dbDataNodes, StringBuilder stringBuilder, Integer num, LocalDate now) {
        if (dbDataNodes.getType() == 1) {
            stringBuilder.append("_").append(now.getYear() + +num);
        }
    }

    private void ruleQuarter(DbDataNodes dbDataNodes, StringBuilder stringBuilder, Integer num, LocalDate now) {
        if (dbDataNodes.getType() == 2) {
            String quarter = now.getYear() + "_" + (now.getMonthValue() / 3 + (now.getMonthValue() % 3 > 0 ? 1 : 0) + num);
            stringBuilder.append("_").append(quarter);
        }
    }

    private void ruleClu(DbDataNodes dbDataNodes, StringBuilder stringBuilder, Integer num) {
        if (dbDataNodes.getType() == 4) {
            stringBuilder.append("_" + num);
        }
    }

    private void ruleMoth(DbDataNodes dbDataNodes, StringBuilder stringBuilder, Integer num, LocalDate now) {
        if (dbDataNodes.getType() == 3) {
            String month = "_" + now.getYear() + "_" + (now.getMonthValue() + num);
            stringBuilder.append(month);
        }
    }


}
