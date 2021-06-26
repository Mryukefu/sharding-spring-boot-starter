package com.dc.game.shardingspringbootstarter.config;

import com.alibaba.druid.filter.Filter;
import com.alibaba.druid.filter.stat.StatFilter;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.support.http.StatViewServlet;
import com.dc.game.shardingspringbootstarter.algorithm.CreateFieldShardingAlgorithm;
import com.dc.game.shardingspringbootstarter.annotation.MyMapperScan;
import com.dc.game.shardingspringbootstarter.entry.enumkey.TableRuleConfigurationEnum;
import com.dc.game.shardingspringbootstarter.event.ActualTableRuleRefreshFromBbEvent;
import com.dc.game.shardingspringbootstarter.wrap.WrapKeyGenerator;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.misc.NotNull;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.shardingsphere.api.config.masterslave.LoadBalanceStrategyConfiguration;
import org.apache.shardingsphere.api.config.masterslave.MasterSlaveRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.*;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@EnableConfigurationProperties({DsProps.class})
@MyMapperScan(basePackages = {"${mybatis.mapperScanner.basePackage}"}, sqlSessionFactoryRef = "sqlSessionFactory",
        sqlSessionTemplateRef = "sqlSessionTemplate")
@ConditionalOnProperty(prefix = "shardingTable", value = {"enable"}, havingValue = "true")
@ComponentScan(basePackageClasses = {WrapKeyGenerator.class,InitializingShadingDb.class,ActualTableRuleRefreshFromBbEvent.class})
public class DataSourceShardingConfig {

    /**
     * 是否开启主从配置
     **/

    @Autowired
    private DsProps dsProps;

    public static final String DB_TABLE = ".";

    @Value("${mybatis.mapper-locations}")
    private String mapperLocations;

    @Value("${mybatis.type-aliases-package}")
    private String typeAliasesPackage;


    /**
     * 阿里数据源拦截器
     *
     * @param
     * @return {@code com.alibaba.druid.filter.Filter}
     * @author ykf
     * @date 2021/6/25 14:35
     */
    @Bean
    public Filter statFilter() {
        StatFilter filter = new StatFilter();
        filter.setSlowSqlMillis(5000);
        filter.setLogSlowSql(true);
        filter.setMergeSql(true);
        return filter;
    }


    /**
     * 这个是Druid监控
     *
     * @param
     * @return {@code org.springframework.boot.web.servlet.ServletRegistrationBean}
     * @author ykf
     * @date 2021/4/12 10:51
     */
    @Bean
    public ServletRegistrationBean statViewServlet() {
        //创建servlet注册实体
        ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean(new StatViewServlet(), "/druid/*");
        //设置ip白名单
        servletRegistrationBean.addInitParameter("allow", "127.0.0.1");
        //设置控制台管理用户
        servletRegistrationBean.addInitParameter("loginUsername", "admin");
        servletRegistrationBean.addInitParameter("loginPassword", "123456");
        //是否可以重置数据
        servletRegistrationBean.addInitParameter("resetEnable", "false");
        return servletRegistrationBean;
    }


    /**
     * 后期如果需要使用多数据源的话可以手动放入ioc 容器
     *
     * @param dsProp
     * @return {@code javax.sql.DataSource}
     * @author ykf
     * @date 2021/4/12 10:49
     */
    public DataSource ds0(@NotNull DsProps.DsProp dsProp) {
        Map<String, Object> dsMap = new HashMap<>();
        Assert.notNull(dsProp.getType(), "没有配置数据源类型");
        Assert.notNull(dsProp.getUrl(), "没有配置数据urL");
        Assert.notNull(dsProp.getUsername(), "没有配置用户");
        Assert.notNull(dsProp.getPassword(), "没有配置密码");
        dsMap.put("type", dsProp.getType());
        dsMap.put("url", dsProp.getUrl());
        dsMap.put("username", dsProp.getUsername());
        dsMap.put("password", dsProp.getPassword());
        DruidDataSource ds = (DruidDataSource) DataSourceUtil.buildDataSource(dsMap);
        ds.setProxyFilters(Lists.newArrayList(statFilter()));
        // 每个分区最大的连接数
        ds.setMaxActive(20);
        // 每个分区最小的连接数
        ds.setMinIdle(5);
        return ds;
    }


    /**
     * 获取sharding 数据源组
     *
     * @return {@code javax.sql.DataSource}
     * @author ykf
     * @date 2021/4/12 10:44
     */
    @Bean("dataSource")
    @Primary
    public DataSource dataSource() throws SQLException {
        //规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        List<DsProps.DsProp> ds = dsProps.getDs();
        Assert.notNull(ds, "没有配置数据库");
        // 配置真实数据源
        Map<String, DataSource> dataSourceMap = new HashMap<>();

        List<DsProps.DsProp> masterSlave = ds.stream().flatMap(t1 -> {
            List<DsProps.DsProp> dcs = new ArrayList<>();
            dcs.add(t1);
            List<DsProps.DsProp> slaveDs = t1.getSlaveDs();
            if (slaveDs != null && slaveDs.size() > 0) {
                dcs.addAll(slaveDs);
            }
            return dcs.stream();
        }).collect(Collectors.toList());
        for (DsProps.DsProp dsProp : masterSlave) {
            log.info("[数据源加载]{}", dsProp.getDcName());
            dataSourceMap.put(dsProp.getDcName(), ds0(dsProp));
        }
         /*List<TableRuleConfigurationEnum> tableRuleConfigurationEnums = Arrays.asList(TableRuleConfigurationEnum.values());
         if (tableRuleConfigurationEnums !=null&&tableRuleConfigurationEnums.size()>0){
            tableRuleConfigurationEnums.forEach(
                    dbNameEnums->{
                        shardingRuleConfig.getTableRuleConfigs().add(ruleConfig(dbNameEnums));
                    }
            );
        }*/
        //  配置主从
        shardingRuleConfig.setMasterSlaveRuleConfigs(getMasterSlaveRuleConfigs(ds));
        // 获取数据源对象
        Properties p = new Properties();
        p.setProperty("sql.show", Boolean.TRUE.toString());

        DataSource dataSource = ShardingDataSourceFactory.createDataSource(dataSourceMap, shardingRuleConfig, p);
        return dataSource;
    }

    //  获取主从配置
    private Collection<MasterSlaveRuleConfiguration> getMasterSlaveRuleConfigs(List<DsProps.DsProp> dsProps) {
        if (dsProps != null && dsProps.size() > 0) {
            return dsProps.stream().map(dsProp -> {
                // 从库
                List<DsProps.DsProp> slaveDs = dsProp.getSlaveDs();
                if (slaveDs != null) {
                    List<String> slaveDataSourceNames = slaveDs.stream().map(DsProps.DsProp::getDcName).collect(Collectors.toList());
                    return new MasterSlaveRuleConfiguration(dsProp.getMsName(), dsProp.getDcName(), slaveDataSourceNames
                            , new LoadBalanceStrategyConfiguration("ROUND_ROBIN"));
                }
                return null;
            }).filter(t1 -> t1 != null).collect(Collectors.toList());
        }
        return null;
    }


    /**
     * 设置表规则
     *
     * @param rEnum
     * @return {@code io.shardingsphere.api.config.rule.TableRuleConfiguration} 配置分表规则
     * @author ykf
     * @date 2021/4/12 10:54
     */

    private TableRuleConfiguration ruleConfig(TableRuleConfigurationEnum rEnum) {
        Assert.notNull(rEnum.getLogicTable(), "没有配置逻辑表");
        Assert.notNull(rEnum.getDbName(), "没有配置数据源名称");
        Assert.notNull(rEnum.getExpression(), "没有配置表达式");
        Assert.notNull(rEnum.getGeneratorColumnName(), "没有指明主键");
        Assert.notNull(rEnum.getShardingColumn(), "没有配置分片键");
        List<DsProps.DsProp> ds = dsProps.getDs();
        Map<String, DsProps.DsProp> dbnameMap = ds.stream().collect(Collectors.toMap(DsProps.DsProp::getDcName, Function.identity()));
        DsProps.DsProp dsProp = dbnameMap.get(rEnum.getDbName());
        Assert.notNull(dsProp, "没有配置数据库");

        // 判断db 是否配置主从
        List<DsProps.DsProp> slaveDs = dsProp.getSlaveDs();
        String dbName = null;
        if (slaveDs != null && slaveDs.size() > 0) {
            dbName = dsProp.getMsName();
        } else {
            dbName = dsProp.getDcName();
        }
        Assert.notNull(dbName, "没有配置数据库");

        TableRuleConfiguration tableRuleConfig = new TableRuleConfiguration(
                rEnum.getLogicTable(), dbName +
                DB_TABLE + rEnum.getLogicTable() + rEnum.getExpression());
        tableRuleConfig.setKeyGeneratorConfig(new KeyGeneratorConfiguration("SNOWFLAKE", rEnum.getGeneratorColumnName()));

        tableRuleConfig.setTableShardingStrategyConfig(new StandardShardingStrategyConfiguration
                (rEnum.getShardingColumn(),
                        new CreateFieldShardingAlgorithm()));
        return tableRuleConfig;
    }

    /**
     * 需要手动配置事务管理器
     *
     * @param dataSource
     * @return {@code org.springframework.jdbc.datasource.DataSourceTransactionManager}
     * @author ykf
     * @date 2021/4/12 10:55
     */
    @Bean
    public DataSourceTransactionManager transactitonManager(@Qualifier("dataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    /**
     * mybatis 使用这个sqlSessionFactory
     *
     * @param dataSource
     * @return {@code org.apache.ibatis.session.SqlSessionFactory}
     * @author ykf
     * @date 2021/4/12 10:55
     */
    @Bean("sqlSessionFactory")
    @Primary
    public SqlSessionFactory sqlSessionFactory(@Qualifier("dataSource") DataSource dataSource) throws Exception {
        SqlSessionFactoryBean bean = getSqlSessionFactoryBean(dataSource);

        SqlSessionFactory sqlSessionFactory = bean.getObject();

        org.apache.ibatis.session.Configuration configuration = sqlSessionFactory.getConfiguration();
        configuration.setMapUnderscoreToCamelCase(true);
        return sqlSessionFactory;
    }

    public SqlSessionFactoryBean getSqlSessionFactoryBean(DataSource dataSource) throws IOException {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();

        bean.setDataSource(dataSource);
        bean.setTypeAliasesPackage(typeAliasesPackage);

        bean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources(mapperLocations));
        return bean;
    }

    @Bean("sqlSessionTemplate")
    @Primary
    public SqlSessionTemplate sqlSessionTemplate(@Qualifier("sqlSessionFactory") SqlSessionFactory
                                                         sqlSessionFactory) {
        return new SqlSessionTemplate(sqlSessionFactory);
    }


}