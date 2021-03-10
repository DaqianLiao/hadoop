package com.fxbin.mybatisplus.service;

import com.fxbin.mybatisplus.bean.User;

/**
 * UserService
 *
 * @author fxbin
 * @version v1.0
 * @since 2019/2/10 1:31
 */
public interface UserService {

    Object listAll(int page, int size);

    int insert(User user);

    int remove(Integer userId);

    int update(User user);

}
