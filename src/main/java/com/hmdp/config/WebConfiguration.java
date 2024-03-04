package com.hmdp.config;

import com.hmdp.interceptor.LoginCheckInterceptor;
import com.hmdp.interceptor.RefreshTokenInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfiguration implements WebMvcConfigurer {
    @Autowired
    RefreshTokenInterceptor refreshTokenInterceptor;
    @Autowired
    LoginCheckInterceptor loginCheckInterceptor;
    
    // Feat: Login authorization with Redis, refresh token -> login check
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(refreshTokenInterceptor)
                .addPathPatterns("/**")
                .order(0);
        registry.addInterceptor(loginCheckInterceptor)
                .excludePathPatterns(
                    "/user/login",
                    "/user/code",
                    "/blog/hot",
                    "/blog/**",
                    "/shop/**",
                    "/shop-type/**",
                    "/upload/**",
                    "/voucher/**"
                )
                .order(1);
    }
}