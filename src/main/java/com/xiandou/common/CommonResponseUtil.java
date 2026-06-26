package com.xiandou.common;

public class CommonResponseUtil {

    public static <T> Result<T> success() {
        return Result.success();
    }

    public static <T> Result<T> success(T data) {
        return Result.success(data);
    }

    public static <T> Result<T> success(String message, T data) {
        return Result.success(data, Result.SUCCESS_CODE, message);
    }

    public static <T> Result<T> fail(String code, String message) {
        return Result.error(code, message);
    }

    public static <T> Result<T> fail(String code, String message, T data) {
        return Result.error(code, message, data);
    }
}
