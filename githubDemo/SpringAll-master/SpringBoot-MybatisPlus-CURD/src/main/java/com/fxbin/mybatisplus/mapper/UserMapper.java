package com.fxbin.mybatisplus.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.fxbin.mybatisplus.bean.User;
import org.apache.ibatis.annotations.Mapper;

/**
 * UserMapper
 *
 * @author fxbin
 * @version v1.0
 * @since 2019/2/10 1:31
 */
@Mapper
public interface UserMapper extends BaseMapper<User> {
}
