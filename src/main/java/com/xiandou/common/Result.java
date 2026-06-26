package com.xiandou.common;

import java.io.Serializable;

public class Result<T> implements Serializable {
    private T data;
    private String code;
    private String message;

    public static final String SUCCESS_CODE = "0";
    public static final String SUCCESS_MESSAGE = "操作成功";

    public Result() {}

    public Result(T data, String code, String message) {
        this.data = data;
        this.code = code;
        this.message = message;
    }

    public static <T> Result<T> success() {
        return success(null, SUCCESS_CODE, SUCCESS_MESSAGE);
    }

    public static <T> Result<T> success(T data) {
        return success(data, SUCCESS_CODE, SUCCESS_MESSAGE);
    }

    public static <T> Result<T> success(T data, String msg) {
        return success(data, SUCCESS_CODE, msg);
    }

    public static <T> Result<T> success(T data, String code, String msg) {
        return new Result<>(data, code, msg);
    }

    public static <T> Result<T> error(String code, String msg) {
        return error(code, msg, null);
    }

    public static <T> Result<T> error(String code, String msg, T data) {
        return new Result<>(data, code, msg);
    }

    public T getData() { return data; }
    public void setData(T data) { this.data = data; }
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
