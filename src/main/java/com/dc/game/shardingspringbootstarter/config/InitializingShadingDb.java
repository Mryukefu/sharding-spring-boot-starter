package com.dc.game.shardingspringbootstarter.config;
import com.dc.game.shardingspringbootstarter.event.ActualTableRuleRefreshFromBbEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import javax.sql.DataSource;


@Configuration
@Order(9999)
@ConditionalOnProperty(prefix = "shardingTable",value = {"enable"},havingValue = "true")
@Slf4j
public class InitializingShadingDb implements InitializingBean {

    @Autowired
    private ActualTableRuleRefreshFromBbEvent actualTableRuleRefreshFromBbEvent;

    @Autowired
    private DataSource dataSource;
    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("===========>项目启动开始刷新表规则");
        actualTableRuleRefreshFromBbEvent.setDataSource(dataSource);
        actualTableRuleRefreshFromBbEvent.updateTable();
        log.info("===========>刷新表规则结束");

    }
}