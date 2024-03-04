local key = KEYS[1]
local threadId = ARGV[1]
local releaseTime = ARGV[2]

-- Determine whether the lock exists
if redis.call('exists', key) == 0 then
    redis.call('hset', key, threadId, '1')
    redis.call('expire', key, releaseTime)
    return 1
end

-- If it is a lock held by current thread, increase the number of reentrant times and refresh the expiration
if redis.call('hexists', key, threadId) == 1 then
    redis.call('hincrby', key, threadId, '1')
    redis.call('expire', key, releaseTime)
    return 1
end

return 0