package com.fxbin.demo.controller;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/7/18
 * time: 22:14
 * description:
 */
@Controller
@EnableAutoConfiguration
public class SampleController {

    @RequestMapping("/")
    @ResponseBody
    String home(){
        return "hello world";
    }

    public static void main(String[] args) {
        SpringApplication.run(SampleController.class);
    }

}
