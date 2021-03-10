package com.fxbin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;

import javax.servlet.MultipartConfigElement;

/**
 * Created with IntelliJ IDEA.
 * User: fxbin
 * Date: 2018/5/22
 * Time: 23:06
 * Description:
 */
@SpringBootApplication
public class Application /*extends SpringBootServletInitializer */{

//    @Override
//    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
//        return application.sources(Application.class);
//    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class, args);
    }


    /**
     * 文件上传配置
     * @return
     */
    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        //单个文件最大   KB,MB
        factory.setMaxFileSize("1MB");
        /// 设置总上传数据总大小
        factory.setMaxRequestSize("1024000KB");
        return factory.createMultipartConfig();
    }

}
