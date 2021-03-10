package com.fxbin.demo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/9/9
 * time: 14:44
 * description:
 */
@RestController
public class TestController {

    @RequestMapping("/test")
    public Object test(){

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "haha");
        map.put("age", 123);
        return map;
    }

}