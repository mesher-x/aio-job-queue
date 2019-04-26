-- KEYS[1] last requeue timestamp
-- ARGV[1] current timestamp
-- ARGV[2] requeue interval

local last_requeue = tonumber(redis.call('get', KEYS[1]))
if last_requeue ~= nil and tonumber(ARGV[1]) - last_requeue <= tonumber(ARGV[2]) then
    return {'error'}
else
    redis.call('set', KEYS[1], ARGV[1])
    return {'ok'}
end
