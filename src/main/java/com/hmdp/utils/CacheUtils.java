package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

// Feat: Cache utils
@Slf4j
@Component
public class CacheUtils {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    
    public String get(String key) {
        return stringRedisTemplate.opsForValue().get(key);
    }
    
    public void set(String key, Object val, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(val), time, unit);
    }
    
    public void setWithLogicalExpiration(String key, Object val, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(val);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    
    public <T, R> R queryWithPassThrough(String keyPrefix, T id, Class<R> type, Long time, TimeUnit unit, Function<T, R> dbFallback) {
        String key = keyPrefix + id;
        
        // Query data from cache
        String dataJson = get(key);
        if (StrUtil.isNotBlank(dataJson)) {
            return JSONUtil.toBean(dataJson, type);
        }
        
        // Handle blank string
        if (dataJson != null) {
            return null;
        }
        
        // Query data from DB
        R data = dbFallback.apply(id);
        if (data == null) {
            // Store a blank string in Redis to prevent cache penetration
            set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        
        // Save data to cache, set expiration time to prevent dirty writing
        set(key, JSONUtil.toJsonStr(data), time, unit);
        
        return data;
    }
    
    public <T, R> R queryWithMutex(String keyPrefix, T id, Class<R> type, Long time, TimeUnit unit, Function<T, R> dbFallback) {
        String key = CACHE_SHOP_KEY + id;
        
        // Query data from cache
        String dataJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(dataJson)) {
            return JSONUtil.toBean(dataJson, type);
        }
        
        // Handle blank string
        if (dataJson != null) {
            return null;
        }
        
        R data;
        String lockKey = LOCK_SHOP_KEY + id;
        try {
            // If obtaining the lock is unsuccessful, then retrieve it again
            if (!tryLock(lockKey)) {
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, time, unit, dbFallback);
            }
            
            // DCL
            dataJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(dataJson)) {
                return JSONUtil.toBean(dataJson, type);
            }
            
            // Query data from DB
            data = dbFallback.apply(id);
            if (data == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            
            // Save data to cache
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(data), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }
        
        return data;
    }
    
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    
    public  <T, R> R queryWithLogicalExpiration(String keyPrefix, T id, Class<R> type, Long time, TimeUnit unit, Function<T, R> dbFallback) {
        String key = CACHE_SHOP_KEY + id;
        
        // Query data from cache
        String dataJson = get(key);
        
        // If the shopJson does not exist, return null
        if (StrUtil.isBlank(dataJson)) {
            return null;
        }
        
        RedisData redisData = JSONUtil.toBean(dataJson, RedisData.class);
        R data = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        
        // If it is not expired, return the result
        if (expireTime.isAfter(LocalDateTime.now())) {
            return data;
        }
        
        // If it is expired, rebuild cache
        String lockKey = LOCK_SHOP_KEY + id;
        if (tryLock(lockKey)) {
            // DCL
            dataJson = get(key);
            if (expireTime.isAfter(LocalDateTime.now())) {
                return JSONUtil.toBean(dataJson, type);
            }
            
            // Open a separate thread to rebuild the cache
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R newData = dbFallback.apply(id);
                    setWithLogicalExpiration(key, newData, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        
        return data;
    }
    
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
