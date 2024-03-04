package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.Tuple;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    
    // Feat: Save verification code to Redis
    // Save code to Redis instead of session to achieve data sharing
    @Override
    public Result sendCode(String phone, HttpSession session) {
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("Phone number is wrong");
        }
        String code = RandomUtil.randomNumbers(6);
        
        // Save code to Redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        
        log.debug("Successfully sent verification code, code is {}", code);
        return Result.ok();
    }
    
    // // Feat: Send verification code with session
    // public Result sendCodeWithSession(String phone, HttpSession session) {
    //     if (RegexUtils.isPhoneInvalid(phone)) {
    //         return Result.fail("Phone number is wrong");
    //     }
    //     String code = RandomUtil.randomNumbers(6);
    //     session.setAttribute("code", code);
    //     log.debug("Successfully sent verification code, code is {}", code);
    //     return Result.ok();
    // }
    
    // Feat: Save user info to Redis
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("Phone number is wrong");
        }
        
        // Get code from Redis
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        
        // Check if the verification code is correct
        if (cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("The verification code is wrong");
        }
        
        // Check if the user exists
        User user = query().eq("phone", phone).one();
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        
        // Generate a UUID as token
        String token = UUID.randomUUID().toString();
        
        // Convert user object to map
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(
            userDTO,
            new HashMap<>(),
            CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString())
        );
        
        // Save user info to Redis
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token, userMap);
        // stringRedisTemplate.expire(LOGIN_USER_KEY + token, LOGIN_USER_TTL, TimeUnit.MINUTES);
        
        log.debug("Save the user, user is {}", user);
        
        // Return token to the front end
        return Result.ok(token);
    }
    
    // Feat: Login with session
    public Result loginWithSession(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("Phone number is wrong");
        }
        
        // Check if the verification code is correct
        String cacheCode = (String) session.getAttribute("code");
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("The verification code is wrong");
        }
        
        User user = query().eq("phone", phone).one();
        
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        
        session.setAttribute("user", user);
        
        log.debug("Save the user, user is {}", user);
        
        return Result.ok();
    }
    
    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        log.debug("Successfully added user, user is {}", user);
        return user;
    }
    
    // Feat: Sign with bitmap
    @Override
    public Result sign() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(":yyyy:MM");
        String key = USER_SIGN_KEY
            + UserHolder.getUser().getId()
            + now.format(formatter);
        int dayOfMonth = now.getDayOfMonth();
        
        // SETBIT user:sign:3:2023:12 25 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }
    
    // Feat: Count sign-in days
    @Override
    public Result signCount() {
        return countMaxConsecutiveSignInsOfMonth();
    }
    
    // Feat: Count total sign-in days of a month
    public Result countTotalSignInsOfMonth() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(":yyyy:MM");
        String key = USER_SIGN_KEY
            + UserHolder.getUser().getId()
            + now.format(formatter);
        
        // BITCOUNT user:sign:3:2023:12
        Long maxLen = stringRedisTemplate.execute((RedisCallback<Long>) connection ->
            connection.bitCount(key.getBytes())
        );
        
        return Result.ok(maxLen);
    }
    
    // Feat: Count max consecutive sign-in days of a month
    //  https://www.bilibili.com/video/BV1cr4y1671t?p=93
    public Result countMaxConsecutiveSignInsOfMonth() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(":yyyy:MM");
        String key = USER_SIGN_KEY
            + UserHolder.getUser().getId()
            + now.format(formatter);
        int dayOfMonth = now.getDayOfMonth();
        
        // BITFIELD user:sign:3:2023:12 GET u25 0
        List<Long> resList = stringRedisTemplate.opsForValue().bitField(
            key,
            BitFieldSubCommands
                .create()
                .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                .valueAt(0)
        );
        if (resList == null || resList.isEmpty()) {
            return Result.ok(0);
        }
        
        Long num = resList.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        
        Long maxLen = 0L;
        Long curLen = 0L;
        num <<= 1;
        while (num != 0) {
            if ((num & 1) == 1) {
                curLen++;
            } else {
                maxLen = Math.max(maxLen, curLen);
                curLen = 0L;
            }
            num >>>= 1;
        }
        
        return Result.ok(maxLen);
    }
}

