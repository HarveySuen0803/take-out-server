package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.RedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    ISeckillVoucherService seckillVoucherService;
    @Autowired
    RedisIdWorker redisIdWorker;
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @Autowired
    RedissonClient redissonClient;
    
    // Feat: Seckill
    @Override
    public Result seckillVoucher(Long voucherId) {
        return handleWithRedisson(voucherId);
    }
    
    // Feat: Seckill with single service
    public Result handleWithSingleService(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) return Result.fail("No voucher");
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) return Result.fail("Has not yet started");
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) return Result.fail("Has ended");
        if (voucher.getStock() < 1) return Result.fail("Insufficient inventory");
        
        // !!! Using userId as a lock, the same user is not allowed to purchase repeatedly, different users will not be affected
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()) {
            // !!! AOP dynamic proxy, resolve the issue of transaction invalidation
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }
    
    // Feat: Seckill with multi services, using distributed mutex of Redis
    public Result handleWithCustomizedLock(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) return Result.fail("No voucher");
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) return Result.fail("Has not yet started");
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) return Result.fail("Has ended");
        if (voucher.getStock() < 1) return Result.fail("Insufficient inventory");
        
        // Obtain a distributed lock to implement serial operation between multiple services
        RedisLock redisLock = new RedisLock(
            LOCK_VOUCHER_KEY + UserHolder.getUser().getId(),
            LOCK_VOUCHER_TTL,
            stringRedisTemplate
        );
        boolean isLock = redisLock.tryLock();
        if (!isLock) {
            return Result.fail("Do not allow duplicate orders");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            redisLock.unLock();
        }
    }
    
    // Feat: Seckill with multi services, using distributed mutex of Redisson
    public Result handleWithRedisson(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) return Result.fail("No voucher");
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) return Result.fail("Has not yet started");
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) return Result.fail("Has ended");
        if (voucher.getStock() < 1) return Result.fail("Insufficient inventory");
        
        // Obtain a distributed lock to implement serial operation between multiple services
        RLock lock = redissonClient.getLock(LOCK_VOUCHER_KEY + UserHolder.getUser().getId());
        boolean isLock = lock.tryLock();
        if (!isLock) {
            return Result.fail("Do not allow duplicate orders");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }
    
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // A person is only allowed to get one voucher
        Long userId = UserHolder.getUser().getId();
        int count = lambdaQuery()
            .eq(VoucherOrder::getUserId, userId)
            .eq(VoucherOrder::getVoucherId, voucherId)
            .count();
        if (count > 0) return Result.fail("The user has already made a purchase once");
        
        // Get the voucher, ensure thread safety through MySQL's optimistic lock
        boolean isUpdated = seckillVoucherService
            .lambdaUpdate()
            .setSql("stock = stock - 1")
            .eq(SeckillVoucher::getVoucherId, voucherId)
            .gt(SeckillVoucher::getStock, 0)
            .update();
        if (!isUpdated) return Result.fail("Insufficient inventory");
        
        // Generate unique ID by RedisWorker
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        
        return Result.ok(orderId);
    }
    
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("SeckillWithMqOfJdk.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    
    IVoucherOrderService proxy;
    
    // Feat: Seckill with multi services, optimize performance using Redis and JDK's MQ
    public Result handleWithMqOfJdk(Long voucherId) {
        int res = stringRedisTemplate.execute(
            SECKILL_SCRIPT,
            Collections.emptyList(),
            voucherId.toString(),
            UserHolder.getUser().getId().toString()
        ).intValue();
        
        if (res == 1) {
            return Result.fail("Insufficient inventory");
        } else if (res == 2) {
            return Result.fail("Cannot place duplicate orders");
        }
        
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        orderTasks.add(voucherOrder);
        
        return Result.ok(orderId);
    }
    
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    
    @PostConstruct
    private void init() {
        // SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandlerWithMqOfJdk());
        // SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandlerWithMqOfRedis());
    }
    
    private class VoucherOrderHandlerWithMqOfJdk implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("Order handler exception", e);
                }
            }
        }
        
        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            RLock lock = redissonClient.getLock(LOCK_ORDER_KEY + userId);
            boolean isLock = lock.tryLock();
            if (!isLock) {
                log.error("Do not allow duplicate orders");
                return;
            }
            try {
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }
    }
    
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // A person is only allowed to get one voucher
        Long userId = voucherOrder.getUserId();
        int count = lambdaQuery()
            .eq(VoucherOrder::getUserId, userId)
            .eq(VoucherOrder::getVoucherId, voucherOrder.getVoucherId())
            .count();
        if (count > 0) return;
        
        // Get the voucher, ensure thread safety through MySQL's optimistic lock
        boolean isUpdated = seckillVoucherService
            .lambdaUpdate()
            .setSql("stock = stock - 1")
            .eq(SeckillVoucher::getVoucherId, voucherOrder.getVoucherId())
            .gt(SeckillVoucher::getStock, 0)
            .update();
        if (!isUpdated) return;
        
        // Generate unique ID by RedisWorker
        save(voucherOrder);
    }
    
    private static final DefaultRedisScript<Long> DEFAULT_REDIS_SCRIPT;
    static {
        DEFAULT_REDIS_SCRIPT = new DefaultRedisScript<>();
        DEFAULT_REDIS_SCRIPT.setLocation(new ClassPathResource("SeckillWithMqOfRedis.lua"));
        DEFAULT_REDIS_SCRIPT.setResultType(Long.class);
    }
    
    // Feat: Seckill with multi services, optimize performance using Redis and Redis's MQ
    public Result handleWithMqOfRedis(Long voucherId) {
        System.out.println(UserHolder.getUser().getId());
        long orderId = redisIdWorker.nextId("order");
        int res = stringRedisTemplate.execute(
            DEFAULT_REDIS_SCRIPT,
            Collections.emptyList(),
            String.valueOf(orderId),
            voucherId.toString(),
            UserHolder.getUser().getId().toString()
        ).intValue();
        
        if (res == 1) {
            return Result.fail("Insufficient inventory");
        } else if (res == 2) {
            return Result.fail("Cannot place duplicate orders");
        }
        
        return Result.ok(orderId);
    }
    
    private class VoucherOrderHandlerWithMqOfRedis implements Runnable {
        String queueName = "stream.orders";
        
        @Override
        public void run() {
            while (true) {
                try {
                    // Get the message from the message queue
                    // XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                        StreamOffset.create(queueName, ReadOffset.from(">"))
                    );
                    
                    // If no message is obtained, continue the loop
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    
                    // After getting the message, complete the order task
                    MapRecord<String, Object, Object> mapRecord = list.get(0);
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(mapRecord.getValue(), new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    
                    // Acknowledge the message
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", mapRecord.getId());
                } catch (Exception e) {
                    log.error("Order handler exception", e);
                    handlePendingList();
                }
            }
        }
        
        private void handlePendingList() {
            while (true) {
                try {
                    // Get the message from the pending list
                    // XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    
                    // If no message is obtained, break the loop
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    
                    // After getting the message, complete the order task
                    MapRecord<String, Object, Object> mapRecord = list.get(0);
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(mapRecord.getValue(), new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    
                    // Acknowledge the message
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", mapRecord.getId());
                } catch (Exception e) {
                    log.error("Pending list handler exception", e);
                }
            }
        }
        
        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            RLock lock = redissonClient.getLock(LOCK_ORDER_KEY + userId);
            boolean isLock = lock.tryLock();
            if (!isLock) {
                log.error("Do not allow duplicate orders");
                return;
            }
            try {
                // A person is only allowed to get one voucher
                int count = lambdaQuery()
                    .eq(VoucherOrder::getUserId, userId)
                    .eq(VoucherOrder::getVoucherId, voucherOrder.getVoucherId())
                    .count();
                if (count > 0) return;
                
                // Get the voucher, ensure thread safety through MySQL's optimistic lock
                boolean isUpdated = seckillVoucherService
                    .lambdaUpdate()
                    .setSql("stock = stock - 1")
                    .eq(SeckillVoucher::getVoucherId, voucherOrder.getVoucherId())
                    .gt(SeckillVoucher::getStock, 0)
                    .update();
                if (!isUpdated) return;
                
                // Generate unique ID by RedisWorker
                save(voucherOrder);
            } finally {
                lock.unlock();
            }
        }
    }
}