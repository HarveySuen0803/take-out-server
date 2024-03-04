package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_LIST_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_LIST_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    
    // Feat: Query data from cache
    @Override
    public Result queryTypeList() {
        // Query data from cache
        String shopTypeListJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_LIST_KEY);
        if (StrUtil.isNotBlank(shopTypeListJson)) {
            List<ShopType> shopTypeList = JSONUtil.toList(JSONUtil.parseArray(shopTypeListJson), ShopType.class);
            return Result.ok(shopTypeList);
        }
        
        // Query data from DB
        List<ShopType> shopTypeList = list();
        if (shopTypeList == null) {
            return Result.fail("Shop type does not exists");
        }
        
        
        HashSet<Object> objects = new HashSet<>();
        Iterator<Object> iterator = objects.iterator();
        // Save data to cache, set expiration time to prevent dirty writing
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_LIST_KEY, JSONUtil.toJsonStr(shopTypeList), CACHE_SHOP_TYPE_LIST_TTL, TimeUnit.MINUTES);
        
        return Result.ok(shopTypeList);
    }
}
