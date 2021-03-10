package com.fxbin.demo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/7/18
 * time: 22:29
 * description:
 */
@RestController
@RequestMapping("/demo")
public class DemoController {

    @RequestMapping("/test")
    public String test(){
        return "Hello World";
    }
}
