package com.fxbin.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created with IntelliJ IDEA.
 * User: fxbin
 * Date: 2018/5/15
 * Time: 22:17
 * Description:
 */
@RestController
public class SampleController {

    @RequestMapping("/")
    String home() {
        return "Hello World!";
    }


}
