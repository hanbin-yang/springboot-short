package com.xiandou.redis.constant;

public class RedisKeyConstant {
    private static final String NAME_PREFIX = "redistemplate_";

    public static final String CHANNEL_PATTERN = NAME_PREFIX + "channel_*";

    public static final String PREFIX_LOCK = "{lock}:";
    public static final String CHANNEL_LOCK_PREFIX = NAME_PREFIX + "channel_lock:{lock}:";

    public static final String PREFIX_SEMAPHORE = "{semaphore}:";
    public static final String CHANNEL_SEMAPHORE_PREFIX = NAME_PREFIX + "sc:{semaphore}:";

    public static final String PREFIX_LATCH = "{latch}:";
    public static final String CHANNEL_LATCH_PREFIX = NAME_PREFIX + "channel_latch:{latch}:";

    public static final String PREFIX_RWLOCK = "{rwlock}:";

    private RedisKeyConstant() {}

    public static String lockKey(String name) { return PREFIX_LOCK + name; }
    public static String lockChannel(String name) { return CHANNEL_LOCK_PREFIX + name; }
    public static String semaphoreKey(String name) { return PREFIX_SEMAPHORE + name; }
    public static String semaphoreChannel(String name) { return CHANNEL_SEMAPHORE_PREFIX + name; }
    public static String latchKey(String name) { return PREFIX_LATCH + name; }
    public static String latchChannel(String name) { return CHANNEL_LATCH_PREFIX + name; }
    public static String rwlockKey(String name) { return PREFIX_RWLOCK + name; }
}
