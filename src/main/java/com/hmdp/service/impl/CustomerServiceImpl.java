package com.hmdp.service.impl;

import com.hmdp.entity.Customer;
import com.hmdp.utils.RedisBloomFilterUtils;
import com.hmdp.mapper.CustomerMapper;
import com.hmdp.service.ICustomerService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import static com.hmdp.utils.RedisConstants.CACHE_CUSTOMER_KEY;

@Service
public class CustomerServiceImpl extends ServiceImpl<CustomerMapper, Customer> implements ICustomerService {
    @Autowired
    RedisTemplate redisTemplate;
    @Autowired
    RedisBloomFilterUtils redisBloomFilterUtils;
    
    public void add(Customer customer) {
        boolean isSave = save(customer);
        
        if (!isSave) {
            throw new RuntimeException("Failed to save customer");
        }
        
        String key = CACHE_CUSTOMER_KEY + customer.getId();
        Customer val = getById(customer.getId());
        redisTemplate.opsForValue().set(key, val);
    }
    
    public Customer queryById(Integer id) {
        String key = CACHE_CUSTOMER_KEY + id;
        
        // Check with bloom filter before query
        if (!redisBloomFilterUtils.check("whitelist", key)) {
            return null;
        }
        
        Customer customer = (Customer) redisTemplate.opsForValue().get(key);
        if (customer == null) {
            customer = getById(id);
            if (customer != null) {
                redisTemplate.opsForValue().set(key, customer);
            }
        }
        return customer;
    }
}
