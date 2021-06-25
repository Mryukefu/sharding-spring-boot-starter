package com.dc.game.shardingspringbootstarter.mapper;

import com.dc.game.shardingspringbootstarter.entry.po.DcUser;
import tk.mybatis.mapper.common.BaseMapper;
import tk.mybatis.mapper.common.special.InsertUseGeneratedKeysMapper;

/**
 * @Description: 用户mapper
 *
 * @author xub
 * @date 2019/10/10 下午8:52
 */
public interface UserMapper extends InsertUseGeneratedKeysMapper<DcUser>, BaseMapper<DcUser> {


}