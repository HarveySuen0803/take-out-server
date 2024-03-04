local orderId = ARGV[1]
local voucherId = ARGV[2]
local userId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- Determine whether the inventory is sufficient.
if tonumber(redis.call('GET', stockKey)) <= 0 then
    return 1
end

-- Determine whether the user repeats the order, a person is only allowed to get one voucher
if redis.call('SISMEMBER', orderKey, userId) == 1 then
    return 2
end

-- Inventory reduction
redis.call('INCRBY', stockKey, 1)
-- Generate order
redis.call('SADD', orderKey, userId)
redis.call('XADD', 'stream.orders', '*', 'id', orderId, 'userId', userId, 'voucherId', voucherId)
return 0