local key = KEYS[1]
local threadId = ARGV[1]
local releaseTime = ARGV[2]

-- Determine whether the lock is held by current thread
if redis.call('hexists', key, threadId) == 0 then
    return nil
end

-- Decrease the number of reentrant times, determine whether it is already on the outermost layer
if redis.call('hincrby', key, threadId, '-1') == 0 then
    redis.call('del', key)
    return nil
else
    redis.call('expire', key, releaseTime)
    return nil
end
