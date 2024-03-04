package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

// Feat: Bloom Filter with Redis's Bitmap
@Component
public class RedisBloomFilterUtils {
    @Autowired
    RedisTemplate redisTemplate;
    
    // @PostConstruct
    public void init() {
        String key1 = "customer:1";
        String key2 = "customer:2";
        String key3 = "customer:3";
        
        int hash1 = Math.abs(key1.hashCode());
        int hash2 = Math.abs(key2.hashCode());
        int hash3 = Math.abs(key3.hashCode());
        
        long index1 = (long) (hash1 % Math.pow(2, 32));
        long index2 = (long) (hash2 % Math.pow(2, 32));
        long index3 = (long) (hash3 % Math.pow(2, 32));
        
        redisTemplate.opsForValue().setBit("whitelist", index1, true);
        redisTemplate.opsForValue().setBit("whitelist", index2, true);
        redisTemplate.opsForValue().setBit("whitelist", index3, true);
    }
    
    public boolean check(String checkItem, String key) {
        int hash = Math.abs(key.hashCode());
        long index = (long) (hash % Math.pow(2, 32));
        return Boolean.TRUE.equals(redisTemplate.opsForValue().getBit(checkItem, index));
    }
}
