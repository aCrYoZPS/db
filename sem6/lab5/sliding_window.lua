local key = KEYS[1]
local window_size = tonumber(ARGV[1])
local max_requests = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local req_id = ARGV[4]

local window_start = now - window_size
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)
local count = redis.call('ZCARD', key)

if count >= max_requests then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry = 0
    if #oldest > 0 then
        retry = math.ceil((tonumber(oldest[2]) + window_size - now) / 1000)
    end
    return { 0, count, retry }
end

redis.call('ZADD', key, now, req_id)
redis.call('PEXPIRE', key, window_size + 1000)
return { 1, count + 1, 0 }
