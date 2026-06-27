package com.xiandou.utils.lock;

/**
 * @author YangHanBin
 * @date 2021-03-16 11:09
 */
public class RedisLockResult<V> {
    private boolean lockResult;

    private V obj;

    public RedisLockResult() {
        this.lockResult = true;
    }

    public RedisLockResult(boolean flag) {
        this.lockResult = flag;
    }

    public RedisLockResult(V obj) {
        this.lockResult = true;
        this.obj = obj;
    }

    public RedisLockResult(boolean lockResult, V obj) {
        this.lockResult = lockResult;
        this.obj = obj;
    }

    public static <V> RedisLockResult<V> fail() {
        return new RedisLockResult<>(false);
    }

    public static <V> RedisLockResult<V> success(V obj) {
        return new RedisLockResult<>(obj);
    }

    public boolean isFailure() {
        return !lockResult;
    }

    public V getObj() {
        return obj;
    }

    public void setObj(V obj) {
        this.obj = obj;
    }
}
