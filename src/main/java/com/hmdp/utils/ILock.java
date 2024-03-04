package com.hmdp.utils;

public interface ILock {
    boolean tryLock();
    
    void unLock();
}
