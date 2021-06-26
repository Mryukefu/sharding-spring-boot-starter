package com.dc.game.shardingspringbootstarter.mapper;

import com.dc.game.shardingspringbootstarter.entry.po.DcUser;
import tk.mybatis.mapper.common.BaseMapper;
import tk.mybatis.mapper.common.special.InsertUseGeneratedKeysMapper;

/**
 * @Description: 用户mapper
 *
 * @author ykf
 * @date 2021/4/16 16:17
 */
public interface UserMapper extends InsertUseGeneratedKeysMapper<DcUser>, BaseMapper<DcUser> {


}