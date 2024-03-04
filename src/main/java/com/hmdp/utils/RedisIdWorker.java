package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

// Feat: Global ID Generator
@Component
public class RedisIdWorker {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    
    private static final long BEGIN_TIMESTAMP = 1691020800;
    private static final int COUNT_BITS = 32;
    
    public long nextId(String keyPrefix) {
        LocalDateTime now = LocalDateTime.now();
        
        // Generate timestamp
        long timestamp = now.toEpochSecond(ZoneOffset.UTC) - BEGIN_TIMESTAMP;
        
        // Generate serial id
        long serialId = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + now.format(DateTimeFormatter.ofPattern(":yyyy:MM:dd")));
        
        return timestamp << COUNT_BITS | serialId;
    }
}
