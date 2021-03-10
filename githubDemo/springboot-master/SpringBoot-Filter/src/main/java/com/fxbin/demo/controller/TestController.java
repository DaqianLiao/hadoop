package com.fxbin.demo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/9/9
 * time: 14:13
 * description:
 */
@RestController
public class TestController {

    @RequestMapping("/test")
    public String test(){
        return "Hello World!";
    }

}