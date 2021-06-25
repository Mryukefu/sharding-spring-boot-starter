package com.dc.game.shardingspringbootstarter.event;

import com.dc.game.shardingspringbootstarter.config.DsProps;
import com.dc.game.shardingspringbootstarter.config.TableRuleConfigurationFactionBuilder;
import com.dc.game.shardingspringbootstarter.entry.po.DbDataNodes;
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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;
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


    public Boolean isMasterSlave() {

        return false;

    }

    /**
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

    public void actualDataNodesRefresh() throws NoSuchFieldException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        ShardingRule shardingRule = shardingDataSource.getRuntimeContext().getRule();
        ShardingConnection connection = shardingDataSource.getConnection();
        // 查询数据库配置的分表节点
        List<DbDataNodes> dbDataNodes = queryCreateTable(connection);

        //  校验数据库配置
        checkDataBaseConfig(dbDataNodes);
        boolean masterSalve = isMasterSalve(shardingRule);


        // 封装实际表(包含数据库节点 loginTableName+后缀)
        List<DbDataNodes> calculateRequiredTables = calculateRequiredTableNames(dbDataNodes);

        Collection<TableRule> newTableRules = structureNewTableRule(calculateRequiredTables);

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

        Set<String> existedTableNames = getExistedTableNames(dbDataNodes, connection);
        calculateRequiredTables = calculateRequiredTables.stream()
                .filter(t1 -> !existedTableNames.contains(t1.getAuthenticTableName()))
                .collect(Collectors.toList());

        if (calculateRequiredTables != null && calculateRequiredTables.size() > 0) {
            createTable(calculateRequiredTables);
        }
    }

    private boolean isMasterSalve(ShardingRule shardingRule) {
        Collection<MasterSlaveRule> masterSlaveRules = shardingRule.getMasterSlaveRules();
        return masterSlaveRules!=null&&masterSlaveRules.size()>0;
    }


    /**
     * desc 创建实际上的节点，样式 dbName.tableName
     *
     * @param type                    作用范围 1 用来生成表 2 用来刷新表
     * @param calculateRequiredTables 数据库配置的分表信息
     * @return {@code java.util.Collection<io.shardingsphere.core.rule.TableRule>}
     * @author ykf
     * @date 2021/4/16 17:17
     */
    private Collection<TableRule> structureNewTableRule(List<DbDataNodes> calculateRequiredTables)
            throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<TableRule> tableRules = new ArrayList<>();

        //  根据逻辑表进行分组
        Map<String, List<DbDataNodes>> map = calculateRequiredTables.stream()
                .collect(Collectors.groupingBy(DbDataNodes::getLogicTableName));

        for (String logicTableName : map.keySet()) {
            List<DbDataNodes> dbDataNodes = map.get(logicTableName);

            //  数据库规则
            TableRule tableRule = new TableRule("", logicTableName);

            //  设置新节点
            List<DataNode> newDataNodes = new ArrayList<>();

            //  设计数据库索引
            Map<DataNode, Integer> dataNodeIndexMap = Maps.newHashMap();

            //  逻辑库对应的表结构
            Map<String, List<String>> dataTableMap = new LinkedHashMap<>();


            List<String> tableName = new ArrayList<>();


            for (DbDataNodes dbDataNode : dbDataNodes) {
                String masterSlaveDateSourceName = dbDataNode.getMasterSlaveDateSourceName();
                DataNode dataNode = null;
                // 如果配置了主从
                if (StringUtil.isNotEmpty(masterSlaveDateSourceName) && masterSlaveDateSourceName.equals(dsProps.getDs().get(0).getMsName())) {
                    dataNode = new DataNode(String.format("%s.%s", masterSlaveDateSourceName, dbDataNode.getAuthenticTableName()));
                    //  没有配置主从的 直接取主库
                } else {
                    dataNode = new DataNode(String.format("%s.%s", dbDataNode.getMasterDataSourceName(), dbDataNode.getAuthenticTableName()));
                }
                dataNodeIndexMap.put(dataNode, dbDataNodes.indexOf(dbDataNode));

                tableName.add(dataNode.getTableName());
                newDataNodes.add(dataNode);
            }

            DbDataNodes dbDataNodes1 = dbDataNodes.get(0);
            dataTableMap.put(dbDataNodes1.getMasterSlaveDateSourceName(), tableName);
            //  获取修改权限
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);

            // 反射刷新actualDataNodes
            Field actualDatasourceNamesField = TableRule.class.getDeclaredField("actualDatasourceNames");
            actualDatasourceNamesField.setAccessible(true);
            modifiersField.setInt(actualDatasourceNamesField, actualDatasourceNamesField.getModifiers() & Modifier.FINAL);
            actualDatasourceNamesField.set(tableRule, dbDataNodes.stream().map(DbDataNodes::getMasterDataSourceName).collect(Collectors.toSet()));

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
            tableShardingStrategyField.set(tableRule, TableRuleConfigurationFactionBuilder
                    .buildShardingStrategyConfiguration(dbDataNodes1.getClassNameShardingStrategy(),
                            dbDataNodes1.getClassNameShardingAlgorithm(), dbDataNodes1.getShardingColumns(), null));

            // 反射刷新  generateKeyColumn
            Field generateKeyColumnField = TableRule.class.getDeclaredField("generateKeyColumn");
            generateKeyColumnField.setAccessible(true);
            modifiersField.setInt(generateKeyColumnField, generateKeyColumnField.getModifiers() & Modifier.FINAL);
            generateKeyColumnField.set(tableRule, dbDataNodes1.getGeneratorColumnName());


            // 反射刷新  shardingKeyGenerator
            Field shardingKeyGeneratorField = TableRule.class.getDeclaredField("shardingKeyGenerator");
            shardingKeyGeneratorField.setAccessible(true);
            modifiersField.setInt(shardingKeyGeneratorField, shardingKeyGeneratorField.getModifiers() & Modifier.FINAL);
            shardingKeyGeneratorField.set(tableRule, Class.forName(dbDataNodes1.getKeyGenerator()).newInstance());


            // 反射刷新  datasourceToTablesMap
            Field datasourceToTablesMapField = TableRule.class.getDeclaredField("datasourceToTablesMap");
            datasourceToTablesMapField.setAccessible(true);
            modifiersField.setInt(datasourceToTablesMapField, datasourceToTablesMapField.getModifiers() & Modifier.FINAL);
            datasourceToTablesMapField.set(tableRule, dataTableMap);
            tableRules.add(tableRule);
        }
        return tableRules;

    }

    /**
     * 项目启动的时候创建分表信息，且刷新节点
     *
     * @return {@code void}
     * @author ykf
     * @date 2021/4/16 17:18
     */
    public void afterPropertiesSet() throws Exception {

        // 查询需要的表
        ShardingConnection connection = shardingDataSource.getConnection();
        List<DbDataNodes> list = queryCreateTable(connection);

        //  校验数据库与配置是否一致
       // checkDataBaseConfig(list);

        List<DbDataNodes> calculateRequiredTables = calculateRequiredTableNames(list);

        //  查询已经存在的表结构
        Set<String> existedTableNames = getExistedTableNames(list, connection);
        calculateRequiredTables = calculateRequiredTables.stream()
                .filter(t1 -> !existedTableNames.contains(t1.getAuthenticTableName()))
                .collect(Collectors.toList());

        // 创建表
        createTable(calculateRequiredTables);

        //  刷新表节点
        actualDataNodesRefresh();
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

        Assert.notNull(dsProps, "请配置数据源属性");
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
        }



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
    public void createTable(List<DbDataNodes> tableNames) {
        if (tableNames != null && tableNames.size() > 0) {
            try (ShardingConnection connection = shardingDataSource.getConnection(); Statement statement = connection
                    .getConnection("new_dc_sdk").createStatement()) {
                for (DbDataNodes tableName : tableNames) {
                    statement.executeUpdate(tableName.getCreateTableTemplate());
                }
                log.info("sharding jdbc [创建表详情]：=>新建表：总数:{},表名称{}", tableNames.size(), tableNames
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
     * @param list       数据库列表，
     * @param connection
     * @return {@code java.util.List<java.lang.String>}
     * @author ykf
     * @date 2021/4/16 17:19
     */
    private Set<String> getExistedTableNames(List<DbDataNodes> list, Connection connection) {
        Set<String> result = new HashSet<>();
        try {
            ShardingConnection shardingConnection = (ShardingConnection) connection;
            Connection masterConnection = shardingConnection.getConnection("new_dc_sdk_master");
            Assert.notNull(masterConnection, "数据源不存在");
            DatabaseMetaData metaData = masterConnection.getMetaData();
            for (DbDataNodes dbDataNodes : list) {
                ResultSet tables = metaData.getTables(dbDataNodes.getMasterDataSourceName(), null,
                        dbDataNodes.getLogicTableName() + "%", new String[]{"TABLE"});
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    result.add("new_dc_sdk" + DB_TABLE + tableName);
                }
            }
        } catch (Exception e) {
            log.error("[查询表失败]", e);
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
    private List<DbDataNodes> queryCreateTable(ShardingConnection connection) {
        List<DbDataNodes> list = new ArrayList<>();
        //  查询需要创建的表
        try {
            Connection masterConnection = connection.getConnection("new_dc_sdk_master");
            Statement statement = masterConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM  db_data_nodes  where  state = 1");
            while (resultSet.next()) {
                DbDataNodes dbDataNodes = new DbDataNodes();
                dbDataNodes.setMasterSlaveDateSourceName(resultSet.getString("master_slave_data_source_name"));
                dbDataNodes.setMasterDataSourceName(resultSet.getString("master_data_source_name"));
                dbDataNodes.setSlaveDateSourceNames(resultSet.getString("slave_data_source_name"));

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
