package com.hmdp.utils;

import org.redisson.api.RBloomFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

// Feat: Bloom Filter with Redisson
@Component
public class RedissonBloomFilterUtils {
    @Autowired
    private RBloomFilter bloomFilter;
    
    // @PostConstruct
    public void init() {
        bloomFilter.add("user:1");
        bloomFilter.add("user:2");
        bloomFilter.add("user:3");
    }
    
    public boolean check(String key) {
        return bloomFilter.contains(key);
    }
}
