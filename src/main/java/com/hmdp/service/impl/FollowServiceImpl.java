package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOWS_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @Autowired
    IUserService userService;
    
    // Feat: follow
    @Override
    public Result follow(Long followUserId, boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = FOLLOWS_KEY + userId;
        
        if (isFollow) {
            // Add follow
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSaved = save(follow);
            
            if (isSaved) {
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            // Remove follow
            boolean isRemoved = lambdaUpdate().eq(Follow::getUserId, userId)
                                    .eq(Follow::getFollowUserId, followUserId)
                                    .remove();
            
            if (isRemoved) {
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }
    
    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        Integer count = lambdaQuery().eq(Follow::getUserId, userId)
                            .eq(Follow::getFollowUserId, followUserId)
                            .count();
        return Result.ok(count);
    }
    
    // Feat: Common follow
    @Override
    public Result followCommon(Long userId1) {
        Long userId2 = UserHolder.getUser().getId();
        String key1 = FOLLOWS_KEY + userId1;
        String key2 = FOLLOWS_KEY + userId2;
        
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if (intersect == null || intersect.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        
        List<Long> idList = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> userDTOList = userService.lambdaQuery()
                                        .in(User::getId, idList)
                                        .list()
                                        .stream()
                                        .map((user) -> BeanUtil.copyProperties(user, UserDTO.class))
                                        .collect(Collectors.toList());
        
        return Result.ok(userDTOList);
    }
}
