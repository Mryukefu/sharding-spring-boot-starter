package com.dc.game.shardingspringbootstarter.controller;

import com.dc.game.shardingspringbootstarter.entry.po.DcUser;
import com.dc.game.shardingspringbootstarter.mapper.UserMapper;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * class desc
 * todo
 *
 * @author ykf
 * @date 2021/6/25 16:23
 */
@RestController
public class TestController {

    @Autowired
    private UserMapper userMapper;

    @GetMapping("test")
    public List<DcUser> test() {
       return Arrays.asList(userMapper.selectByPrimaryKey(3L)) ;


    }

    @GetMapping("test2")
    public Integer test2() {
        Random random = new Random();
        DcUser dcUser = userMapper.selectByPrimaryKey(1L);
        dcUser.setUserId(null);
      //  dcUser.setUserId(random.nextLong());
        dcUser.setUserName(random.nextFloat()+"");
        dcUser.setUserPhone(random.nextFloat()+"");
        dcUser.setUserPassword(random.nextFloat()+"");
        DcUser dcUser1 = new DcUser();
        BeanUtils.copyProperties(dcUser, dcUser1);
      //  dcUser1.setUserId(random.nextLong());
        dcUser.setUserName(random.nextFloat()+"");
        dcUser.setUserPhone(random.nextFloat()+"");
        dcUser.setUserPassword(random.nextFloat()+"");
        List<DcUser> list = new ArrayList<>();
        list.add(dcUser);
        list.add(dcUser1);
        for (DcUser user : list) {
            userMapper.insertSelective(user);
        }
        return 1;

    }
}
