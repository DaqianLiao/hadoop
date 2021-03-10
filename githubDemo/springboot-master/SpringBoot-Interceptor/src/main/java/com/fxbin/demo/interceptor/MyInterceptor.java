package com.fxbin.demo.interceptor;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * created with IntelliJ IDEA.
 * author: fxbin
 * date: 2018/9/9
 * time: 14:48
 * description:
 */
@Component
public class MyInterceptor implements HandlerInterceptor {

    /**
     * 进入controller层之前拦截请求
     * @param request
     * @param response
     * @param handler
     * @return
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        System.out.println("getContextPath:" + request.getContextPath());
        System.out.println("getServletPath:" + request.getServletPath());
        System.out.println("getRequestURI:" + request.getRequestURI());
        System.out.println("getRequestURL:" + request.getRequestURL());
        System.out.println("getRealPath:" + request.getSession().getServletContext().getRealPath("image"));
        return true;
    }


    /**
     * 处理请求完成后视图渲染之前的处理操作
     * @param request
     * @param response
     * @param handler
     * @param modelAndView
     */
    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) {

    }

    /**
     * 视图渲染之后的操作
     * @param request
     * @param response
     * @param handler
     * @param ex
     */
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {

    }
}