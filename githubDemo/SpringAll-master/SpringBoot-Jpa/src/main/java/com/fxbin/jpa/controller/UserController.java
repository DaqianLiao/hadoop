package com.fxbin.jpa.controller;

import com.fxbin.jpa.bean.User;
import com.fxbin.jpa.repository.UserRepository;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * UserController
 *
 * @author fxbin
 * @version v1.0
 * @since 2019/3/5 0:34
 */

@RestController
@RequestMapping("/user")
public class UserController {

    @Resource
    private UserRepository userRepository;

    /**
     * 查询全部
     * @return
     */
    @RequestMapping("/listAll")
    public Object listAll(){
        return userRepository.findAll();
    }

    /**
     * 添加数据
     * @param user
     * @return
     */
    @RequestMapping("/insert")
    public void insert (User user){
        userRepository.save(user);
    }

    /**
     * 删除
     * @param userId
     * @return
     */
    @RequestMapping("/remove")
    public void remove(Integer userId){
        userRepository.deleteById(userId);
    }

    /**
     * 修改
     * @param user
     * @return
     */
    @RequestMapping("/update")
    public void update(User user){
        userRepository.save(user);
    }

}
