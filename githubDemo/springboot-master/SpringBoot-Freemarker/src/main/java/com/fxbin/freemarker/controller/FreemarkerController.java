package com.fxbin.freemarker.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/10/21
 * time: 4:15
 * version: 1.0
 * description:
 */
@Controller
public class FreemarkerController {


    @RequestMapping("/index")
    public String index(Model model){
        model.addAttribute("content", "Freemarker");
        return "index";
    }

}
