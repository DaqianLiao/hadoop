package com.ldq.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author: Daqian Liao
 * @Description:
 * @Date:
 */
@RestController
@RequestMapping("/producer")
public class SendController {

    @Autowired
    private Producer producer;

    @RequestMapping(value = "/send")
    public String send() {
        producer.send();
        return "{\"code\":0}";
    }

    @RequestMapping("/test")
    public String test(){
        return "OK";
    }
}
