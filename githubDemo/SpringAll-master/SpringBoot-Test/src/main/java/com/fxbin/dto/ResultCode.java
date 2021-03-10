package com.fxbin.dto;

/**
 * @author: fxbin
 * @datetime: 2018/7/5 18:36
 * @description:
 */
public enum ResultCode {

    SUCCESS(200),//成功
    FAIL(400),//失败
    UNAUTHORIZED(401),//未认证（签名错误）
    NOT_FOUND(404),//接口不存在
    INTERNAL_SERVER_ERROR(500);//服务器内部错误

    private final int code;

    ResultCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

}