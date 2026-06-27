package com.xiandou.redis.config;

import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

public class LuaScriptRegistry {

    // ==================== 锁 ====================
    public static final RedisScript<Long> LOCK_SCRIPT;
    public static final RedisScript<Long> UNLOCK_SCRIPT;
    public static final RedisScript<Long> RENEW_SCRIPT;

    // ==================== 信号量 ====================
    public static final RedisScript<Long> SEMAPHORE_ACQUIRE_SCRIPT;
    public static final RedisScript<Long> SEMAPHORE_RELEASE_SCRIPT;

    // ==================== CountDownLatch ====================
    public static final RedisScript<Long> SET_IF_ABSENT_SCRIPT;  // 通用"不存在则设置"脚本
    public static final RedisScript<Long> LATCH_COUNTDOWN_SCRIPT;

    static {
        LOCK_SCRIPT = createScript(
            "if (redis.call('exists', KEYS[1]) == 0) then " +
                "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "return nil; " +
            "end; " +
            "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "return nil; " +
            "end; " +
            "return redis.call('pttl', KEYS[1]);",
            Long.class
        );

        UNLOCK_SCRIPT = createScript(
            "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                "return nil; " +
            "end; " +
            "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
            "if (counter > 0) then " +
                "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                "return 0; " +
            "else " +
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; " +
            "end;",
            Long.class
        );

        RENEW_SCRIPT = createScript(
            "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "return 1; " +
            "end; " +
            "return 0;",
            Long.class
        );

        /**
         * 信号量获取 ARGV[1]=permits, ARGV[2]=ttlSeconds
         */
        SEMAPHORE_ACQUIRE_SCRIPT = createScript(
            "local value = redis.call('get', KEYS[1]); " +
            "if (value ~= false and tonumber(value) >= tonumber(ARGV[1])) then " +
                "redis.call('decrby', KEYS[1], ARGV[1]); " +
                "redis.call('expire', KEYS[1], ARGV[2]); " +
                "return 1; " +
            "end; " +
            "return 0;",
            Long.class
        );

        /**
         * 信号量释放 ARGV[1]=permits, ARGV[2]=ttlSeconds
         */
        SEMAPHORE_RELEASE_SCRIPT = createScript(
            "local val = redis.call('incrby', KEYS[1], ARGV[1]); " +
            "redis.call('expire', KEYS[1], ARGV[2]); " +
            "redis.call('publish', KEYS[2], 'release'); " +
            "return val;",
            Long.class
        );

        /**
         * 不存在则设置 ARGV[1]=value, ARGV[2]=ttlSeconds
         */
        SET_IF_ABSENT_SCRIPT = createScript(
            "if redis.call('exists', KEYS[1]) == 0 then " +
                "redis.call('set', KEYS[1], ARGV[1]); " +
                "redis.call('expire', KEYS[1], ARGV[2]); " +
                "return 1; " +
            "end; " +
            "return 0;",
            Long.class
        );

        /**
         * CountDownLatch 递减 ARGV[1]=ttlSeconds
         * 先检查 Key 是否存在（可能已过期），不存在则通知等待线程解除阻塞。
         * 不直接 DECR 避免创建 -1 导致 await 误判。
         */
        LATCH_COUNTDOWN_SCRIPT = createScript(
            "if redis.call('exists', KEYS[1]) == 1 then " +
                "local val = redis.call('decr', KEYS[1]); " +
                "redis.call('expire', KEYS[1], ARGV[1]); " +
                "if val <= 0 then " +
                    "redis.call('publish', KEYS[2], 0); " +
                "end; " +
                "return val; " +
            "end; " +
            "redis.call('publish', KEYS[2], 0); " +
            "return nil;",
            Long.class
        );
    }

    private static <T> RedisScript<T> createScript(String script, Class<T> resultType) {
        DefaultRedisScript<T> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(script);
        redisScript.setResultType(resultType);
        return redisScript;
    }
}
