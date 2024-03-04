package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

// Feat: Customize a distributed lock with Redis
public class RedisLock implements ILock {
    private StringRedisTemplate stringRedisTemplate;
    private String key;
    private Long timeout;
    
    private static final String UUID_PREFIX = UUID.randomUUID().toString(true) + "-"; // hutool
    
    // Get the stringRedisTemplate through the constructor, not through the IOC
    public RedisLock(String key, Long timeout, StringRedisTemplate stringRedisTemplate) {
        this.key = key;
        this.timeout = timeout;
        this.stringRedisTemplate = stringRedisTemplate;
    }
    
    @Override
    public boolean tryLock() {
        // Use the UUID + ThreadID to uniquely identify the thread holding the lock
        String val = UUID_PREFIX + Thread.currentThread().getId();
        
        return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(key, val, timeout, TimeUnit.SECONDS));
    }
    
    
    @Override
    public void unLock() {
        unLockWithLua();
    }

    // Feat: Unlock with thread identifier to avoid unlock by mistake (Service Stuck + Expiration)
    //  https://www.bilibili.com/video/BV1cr4y1671t?p=59
    private void unLockWithNormal() {
        // Get the thread identifier
        String val = stringRedisTemplate.opsForValue().get(key);
        
        // If the lock does not belong to current thread, then return
        if (!val.equals(UUID_PREFIX + Thread.currentThread().getId())) return;
        
        stringRedisTemplate.delete(key);
    }
    
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("UnLock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }
    
    // Feat: UnLock with Lua script to to ensure atomicity to avoid unlock by mistake (GC Stuck + Expiration)
    //  https://www.bilibili.com/video/BV1cr4y1671t?p=61
    private void unLockWithLua() {
        // Get the thread identifier
        String val = stringRedisTemplate.opsForValue().get(key);
        
        // Call Lua script to delete key
        stringRedisTemplate.execute(
            UNLOCK_SCRIPT,
            Collections.singletonList(key),
            val
        );
    }
}
