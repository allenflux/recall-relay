-- 向量沿用对应正文的剩余有效期。如果正文已被淘汰，本次向量写入也随之放弃。
local ttl = redis.call('PTTL', ARGV[1])
if ttl <= 0 then return 0 end
redis.call('SET', ARGV[1] .. ':vector', ARGV[2], 'PX', ttl)
redis.call('ZADD', KEYS[1], 0, ARGV[3])
return 1
