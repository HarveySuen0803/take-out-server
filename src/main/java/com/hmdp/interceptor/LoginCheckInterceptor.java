package com.hmdp.interceptor;

import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Component
public class LoginCheckInterceptor implements HandlerInterceptor {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (UserHolder.getUser() == null) {
            response.setStatus(401);
            return false;
        }
        
        return true;
    }
    
    // // Feat: Login authorization with session
    // @Override
    // public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
    //     // Get user in session
    //     HttpSession session = request.getSession();
    //     User user = (User) session.getAttribute("user");
    //
    //     // If the user does not exist, intercept the request
    //     if (user == null) {
    //         response.setStatus(401);
    //         return false;
    //     }
    //
    //     // If the user exists, stored the user info to ThreadLocal
    //     UserDTO userDTO = new UserDTO();
    //     BeanUtils.copyProperties(user, userDTO);
    //     UserHolder.saveUser(userDTO);
    //
    //     return HandlerInterceptor.super.preHandle(request, response, handler);
    // }
}
