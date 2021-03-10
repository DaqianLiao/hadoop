package com.fxbin.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/9/9
 * time: 12:13
 * description:
 */
@RestController
public class TestController {

    @GetMapping(value="/abc")
    public Object testException(){
        int i = 5/0;
        return i;
    }


}