package com.fxbin.thymeleaf.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/10/21
 * time: 4:42
 * version: 1.0
 * description:
 */
@Controller
public class ThymeleafController {


    @RequestMapping("/test")
    public String test(Model model){
        model.addAttribute("content", "Hello Thymeleaf");
        return "index";
    }


}
