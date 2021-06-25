package com.dc.game.shardingspringbootstarter.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "spring.sharding.datasource")
public class DsProps {
    private List<DsProp> ds;


    @Data
    public static class DsProp{

        /** 主从 组名称**/
        private String msName;

        /** 主库 资源定位地址**/
        private String url;

        /** 主库 用户名称**/
        private String username;

        /** 主库 密码**/
        private String password;

        /** 主库 数据库类型**/
        private String type;

        /** 主库 数据库名称**/
        private String dcName;

        /** 从库 列表**/
        private List<DsProp> slaveDs;



    }


}

