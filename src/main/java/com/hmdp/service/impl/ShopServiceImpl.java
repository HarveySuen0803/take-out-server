package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheUtils;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.DEFAULT_PAGE_SIZE;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @Autowired
    CacheUtils cacheUtils;
    
    @Override
    public Result queryById(Long id) {
        Shop shop = queryWithCacheUtils(id);
        return shop != null ? Result.ok(shop) : Result.fail("Shop does not exists");
    }
    
    // Feat: Query with pass through, and save an blank string to cache to avoid cache penetration
    private Shop queryWithBlank(Long id) {
        String key = CACHE_SHOP_KEY + id;
        
        // Query data from cache
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        
        // Handle blank string
        if (shopJson != null) {
            return null;
        }
        
        // Query data from DB
        Shop shop = getById(id);
        if (shop == null) {
            // Store a blank string in Redis to avoid cache penetration
            stringRedisTemplate.opsForValue()
                .set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        
        // Save data to cache, set expiration time to avoid dirty writing
        stringRedisTemplate.opsForValue()
            .set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        
        return shop;
    }
    
    // Feat: Query with mutex to avoid cache invalidation
    private Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        
        // Query data from cache
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        
        // Handle blank string
        if (shopJson != null) {
            return null;
        }
        
        Shop shop;
        String lockKey = LOCK_SHOP_KEY + id;
        try {
            // If obtaining the lock is unsuccessful, then retrieve it again
            if (!tryLock(lockKey)) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            
            // DCL
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            
            // Query data from DB
            shop = getById(id);
            if (shop == null) {
                stringRedisTemplate.opsForValue()
                    .set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            
            // Save data to cache
            stringRedisTemplate.opsForValue()
                .set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }
        
        return shop;
    }
    
    // Feat: Custom lock with Redis
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
    
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    
    // Feat: Query with logical expiration to avoid cache invalidation
    private Shop queryWithLogicalExpiration(Long id) {
        String key = CACHE_SHOP_KEY + id;
        
        // Query data from cache
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        
        // If the shopJson does not exist, return null
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        
        // If it is not expired, return the result
        if (expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }
        
        // If it is expired, rebuild cache
        String lockKey = LOCK_SHOP_KEY + id;
        if (tryLock(lockKey)) {
            // DCL
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (expireTime.isAfter(LocalDateTime.now())) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            
            // Open a separate thread to rebuild the cache
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    saveShopToRedis(id, 10L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        
        return shop;
    }
    
    private void saveShopToRedis(Long id, Long expireTime) {
        Shop shop = getById(id);
        
        // Set data and logical expiration time
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        
        stringRedisTemplate.opsForValue()
            .set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
    
    // Feat: Query with cache utils
    private Shop queryWithCacheUtils(Long id) {
        // return cacheUtils.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, CACHE_SHOP_TTL, TimeUnit.MINUTES, this::getById);
        // return cacheUtils.queryWithLogicalExpiration(CACHE_SHOP_KEY, id, Shop.class, CACHE_SHOP_TTL, TimeUnit.MINUTES, this::getById);
        return cacheUtils.queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, CACHE_SHOP_TTL, TimeUnit.MINUTES, this::getById);
    }
    
    @Override
    @Transactional
    public Result update(Shop shop) {
        updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }
    
    // Feat: Query shop with geo by type
    //  https://www.bilibili.com/video/BV1cr4y1671t?p=90
    @Override
    public Result queryShopByType(Integer typeId, Integer pageNo, Double x, Double y) {
        if (x == null || y == null) {
            return Result.ok(query()
                .eq("type_id", typeId)
                .page(new Page<>(pageNo, DEFAULT_PAGE_SIZE))
                .getRecords());
        }
        
        String key = SHOP_GEO_KEY + typeId;
        int srtIdx = (pageNo - 1) * DEFAULT_PAGE_SIZE;
        int endIdx = pageNo * DEFAULT_PAGE_SIZE;
        
        // GEOSEARCH key FROMLONLAT x, y BYRADIUS 5000 WITHDIST LIMIT endIdx
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate
            .opsForGeo()
            .search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands
                    .GeoSearchCommandArgs
                    .newGeoSearchArgs()
                    .includeDistance()
                    .limit(endIdx)
            );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();
        if (content.size() <= srtIdx) {
            return Result.ok(Collections.emptyList());
        }
        
        List<Long> shopIdList = new ArrayList<>(content.size());
        Map<String, Distance> distanceMap = new HashMap<>(content.size());
        content.stream().skip(srtIdx).forEach((res) -> {
            String shopId = res.getContent().getName();
            shopIdList.add(Long.valueOf(shopId));
            Distance distance = res.getDistance();
            distanceMap.put(shopId, distance);
        });
        
        // List<String> shopIdList = content.stream()
        //     .map((res) -> res.getContent().getName())
        //     .collect(Collectors.toList());
        // Map<String, Distance> distanceMap = content.stream()
        //     .skip(srtIdx)
        //     .collect(Collectors.toMap((res) -> res.getContent().getName(), GeoResult::getDistance));
        
        List<Shop> shopList = lambdaQuery()
            .in(Shop::getId, shopIdList)
            .last("ORDER BY FIELD (id," + StrUtil.join(",", shopIdList) + ")")
            .list();
        
        for (Shop shop : shopList) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        
        // shopList.forEach((shop) -> shop.setDistance(distanceMap.get(shop.getId().toString()).getValue()));
        
        return Result.ok(shopList);
    }
}
