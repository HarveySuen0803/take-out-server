package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Autowired
    IUserService userService;
    @Autowired
    IFollowService followService;
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    
    @Override
    public Result queryHotBlog(Integer current) {
        Page<Blog> page = query()
                              .orderByDesc("liked")
                              .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        List<Blog> records = page.getRecords();
        records.forEach((blog) -> {
            this.queryBlogById(blog.getId());
            this.queryBlogWithLiked(blog);
        });
        return Result.ok(records);
    }
    
    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("Can not find the blog");
        }
        queryBlogWithUser(blog);
        queryBlogWithLiked(blog);
        return Result.ok(blog);
    }
    
    private void queryBlogWithLiked(Blog blog) {
        if (UserHolder.getUser() == null) {
            return;
        }
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }
    
    private void queryBlogWithUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
    
    // Feat: Thumbs up function with Redis's zset
    @Override
    public Result likeBlog(Long id) {
        // Determine whether the current user has liked
        Long userId = UserHolder.getUser().getId();
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        
        if (score == null) {
            // If the user is not liked, save the user to set
            boolean isUpdated = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isUpdated) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // If the user is liked, remove the user from set
            boolean isUpdated = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isUpdated) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        
        return Result.ok();
    }
    
    // Feat: Get the liked list from Redis's zset
    @Override
    public Result queryBlogLikedList(Long id) {
        String key = BLOG_LIKED_KEY + id;
        
        // Sort according to time and take the first 5 from the zset
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> userIdList = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        
        // Get the user list
        // WHERE id IN (5, 1, 3) -- 1, 3, 5
        // WHERE id IN (5, 1, 3) ORDER BY FIELD (id, 5, 1, 3); -- 5, 1, 3
        List<UserDTO> userDTOList = userService.query()
                                               .in("id", userIdList)
                                               .last("ORDER BY FIELD(id," + StrUtil.join(",", userIdList) + ")")
                                               .list()
                                               .stream()
                                               .map((user) -> BeanUtil.copyProperties(user, UserDTO.class))
                                               .collect(Collectors.toList());
        
        return Result.ok(userDTOList);
    }
    
    // Feat: Feed stream
    @Override
    public Result saveBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        save(blog);
        
        // Get all followers, select * from tb_follow where follow_user_id = ?
        List<Follow> followList = followService.lambdaQuery()
                                .eq(Follow::getFollowUserId, user.getId())
                                .list();
        
        // Feed message to them
        for (Follow follow : followList) {
            String key = FEED_KEY + follow.getId();
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        
        return Result.ok(blog.getId());
    }
    
    // Feat: Query blog of follow through scrolling gesture
    //  https://www.bilibili.com/video/BV1cr4y1671t?p=86
    @Override
    public Result queryBlogOfFollow(Long maxTime, Integer offset) {
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY + userId;
        
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                                                                 .reverseRangeByScoreWithScores(key, 0, maxTime, offset, 2);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        
        // !!! Set the initial size to prevent expansion from affecting performance
        List<Long> blogIdList = new ArrayList<>(typedTuples.size());
        List<Blog> blogList = lambdaQuery().in(Blog::getId, blogIdList)
                              .last("ORDER BY FIELD(id," + StrUtil.join(",", blogIdList) + ")")
                              .list();
        for (Blog blog : blogList) {
            queryBlogWithUser(blog);
            queryBlogWithLiked(blog);
        }
        
        // Get the minTime and offset
        offset = 0;
        long minTime = 0;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            blogIdList.add(Long.valueOf(tuple.getValue()));
            long time = tuple.getScore().longValue();
            if (minTime == time) {
                offset++;
            } else {
                minTime = time;
                offset = 1;
            }
        }
        
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogList);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(offset);
        
        return Result.ok(scrollResult);
    }
}
