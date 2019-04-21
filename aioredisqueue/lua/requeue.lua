-- KEYS[1] ack table
-- KEYS[2] main queue
-- KEYS[3] last requeue timestamp
-- ARGV[1] current timestamp
-- ARGV[2] requeue interval
-- ARGV[3] requeue jobs less than this numeric value

local last_requeue = tonumber(redis.call('get', KEYS[3]))
local now = tonumber(ARGV[1])
if last_requeue ~= nil and now - last_requeue <= tonumber(ARGV[2]) then
    return {'error'}
end

local job_id, v, r
local results_hdel = {}
local results_lpush = {}
local results_keys = {}
local min_val = tonumber(ARGV[3])
local kvs = redis.call('HGETALL', KEYS[1])

for i = 1, #kvs, 2 do
    job_id = kvs[i]
    v = tonumber(kvs[i + 1])
    if min_val == -1 or v < min_val then
        r = redis.call('hdel', KEYS[1], job_id)
        table.insert(results_hdel, r)
        if r then
            table.insert(results_lpush, redis.call('lpush', KEYS[2], job_id))
        else
            -- Actually, that should never happen. But let's say we are looking
            -- into the bright future of multithreaded redis
            table.insert(results_lpush, -1)
        end
        table.insert(results_keys, job_id)
    end
end

redis.call('set', KEYS[3], tostring(now))

return {'ok', results_hdel, results_lpush, results_keys}
