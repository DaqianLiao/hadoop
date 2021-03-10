package com.fxbin.demo.config;

import com.fxbin.demo.interceptor.MyInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/9/9
 * time: 14:46
 * description:
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Bean
    public MyInterceptor myInterceptor(){
        return new MyInterceptor();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        //添加拦截路径和排除拦截路径
        registry.addInterceptor(myInterceptor()).addPathPatterns("/**").excludePathPatterns("/test");
    }
}