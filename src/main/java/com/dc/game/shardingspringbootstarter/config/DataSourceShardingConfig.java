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
     * ????????????????????????
     **/

    @Autowired
    private DsProps dsProps;

    public static final String DB_TABLE = ".";

    @Value("${mybatis.mapper-locations}")
    private String mapperLocations;

    @Value("${mybatis.type-aliases-package}")
    private String typeAliasesPackage;


    /**
     * ????????????????????????
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
     * ?????????Druid??????
     *
     * @param
     * @return {@code org.springframework.boot.web.servlet.ServletRegistrationBean}
     * @author ykf
     * @date 2021/4/12 10:51
     */
    @Bean
    public ServletRegistrationBean statViewServlet() {
        //??????servlet????????????
        ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean(new StatViewServlet(), "/druid/*");
        //??????ip?????????
        servletRegistrationBean.addInitParameter("allow", "127.0.0.1");
        //???????????????????????????
        servletRegistrationBean.addInitParameter("loginUsername", "admin");
        servletRegistrationBean.addInitParameter("loginPassword", "123456");
        //????????????????????????
        servletRegistrationBean.addInitParameter("resetEnable", "false");
        return servletRegistrationBean;
    }


    /**
     * ????????????????????????????????????????????????????????????ioc ??????
     *
     * @param dsProp
     * @return {@code javax.sql.DataSource}
     * @author ykf
     * @date 2021/4/12 10:49
     */
    public DataSource ds0(@NotNull DsProps.DsProp dsProp) {
        Map<String, Object> dsMap = new HashMap<>();
        Assert.notNull(dsProp.getType(), "???????????????????????????");
        Assert.notNull(dsProp.getUrl(), "??????????????????urL");
        Assert.notNull(dsProp.getUsername(), "??????????????????");
        Assert.notNull(dsProp.getPassword(), "??????????????????");
        dsMap.put("type", dsProp.getType());
        dsMap.put("url", dsProp.getUrl());
        dsMap.put("username", dsProp.getUsername());
        dsMap.put("password", dsProp.getPassword());
        DruidDataSource ds = (DruidDataSource) DataSourceUtil.buildDataSource(dsMap);
        ds.setProxyFilters(Lists.newArrayList(statFilter()));
        // ??????????????????????????????
        ds.setMaxActive(20);
        // ??????????????????????????????
        ds.setMinIdle(5);
        return ds;
    }


    /**
     * ??????sharding ????????????
     *
     * @return {@code javax.sql.DataSource}
     * @author ykf
     * @date 2021/4/12 10:44
     */
    @Bean("dataSource")
    @Primary
    public DataSource dataSource() throws SQLException {
        //??????
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        List<DsProps.DsProp> ds = dsProps.getDs();
        Assert.notNull(ds, "?????????????????????");
        // ?????????????????????
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
            log.info("[???????????????]{}", dsProp.getDcName());
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
        //  ????????????
        shardingRuleConfig.setMasterSlaveRuleConfigs(getMasterSlaveRuleConfigs(ds));
        // ?????????????????????
        Properties p = new Properties();
        p.setProperty("sql.show", Boolean.TRUE.toString());

        DataSource dataSource = ShardingDataSourceFactory.createDataSource(dataSourceMap, shardingRuleConfig, p);
        return dataSource;
    }

    //  ??????????????????
    private Collection<MasterSlaveRuleConfiguration> getMasterSlaveRuleConfigs(List<DsProps.DsProp> dsProps) {
        if (dsProps != null && dsProps.size() > 0) {
            return dsProps.stream().map(dsProp -> {
                // ??????
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
     * ???????????????
     *
     * @param rEnum
     * @return {@code io.shardingsphere.api.config.rule.TableRuleConfiguration} ??????????????????
     * @author ykf
     * @date 2021/4/12 10:54
     */

    private TableRuleConfiguration ruleConfig(TableRuleConfigurationEnum rEnum) {
        Assert.notNull(rEnum.getLogicTable(), "?????????????????????");
        Assert.notNull(rEnum.getDbName(), "???????????????????????????");
        Assert.notNull(rEnum.getExpression(), "?????????????????????");
        Assert.notNull(rEnum.getGeneratorColumnName(), "??????????????????");
        Assert.notNull(rEnum.getShardingColumn(), "?????????????????????");
        List<DsProps.DsProp> ds = dsProps.getDs();
        Map<String, DsProps.DsProp> dbnameMap = ds.stream().collect(Collectors.toMap(DsProps.DsProp::getDcName, Function.identity()));
        DsProps.DsProp dsProp = dbnameMap.get(rEnum.getDbName());
        Assert.notNull(dsProp, "?????????????????????");

        // ??????db ??????????????????
        List<DsProps.DsProp> slaveDs = dsProp.getSlaveDs();
        String dbName = null;
        if (slaveDs != null && slaveDs.size() > 0) {
            dbName = dsProp.getMsName();
        } else {
            dbName = dsProp.getDcName();
        }
        Assert.notNull(dbName, "?????????????????????");

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
     * ?????????????????????????????????
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
     * mybatis ????????????sqlSessionFactory
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