package com.fxbin.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/7/18
 * time: 23:17
 * description:
 */
@Controller
public class SampleController {

    @RequestMapping("/hello")
    @CrossOrigin("http://localhost:8080")
    public String hello( ){
        return "Hello World";
    }


}
