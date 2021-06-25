package com.dc.game.shardingspringbootstarter.entry.enumkey;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum BindingTableGroupsEnum {

    ADMIN_DB_TABLE("ADMIN_DB", "dc_user,dc_user_account,dc_user_extend");
    private String tables;

    private String adminType;



    BindingTableGroupsEnum(String adminType, String tables){
        this.adminType = adminType;
        this.tables = tables;

    }

    public String getTables() {
        return tables;
    }

    public String getAdminType() {
        return adminType;
    }

    public static List<String> getTableByAdminType(String adminType) {
        BindingTableGroupsEnum[] values = BindingTableGroupsEnum.values();
        return Arrays.stream(values)
                .filter(t1 -> t1.getAdminType().equals(adminType))
                .map(BindingTableGroupsEnum::getTables).collect(Collectors.toList());

    }





}
