-- Redis 一次执行整个脚本。正文、过期时间和索引同步更新，多个 Relay 进程并发写入时容量上限依然生效。
-- KEYS[1] 按失效时间排序；KEYS[2] 按字典序保存近似候选的“范围:指纹”。
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
local ttl, capacity = tonumber(ARGV[4]), tonumber(ARGV[5])
local key = ARGV[1] .. ARGV[2]
local member = ARGV[3] .. ':' .. ARGV[2]
-- MySQL 回填只能补缺。NX 保证已有的热向量优先，PX 让向量随正文到期。
local function restore_vector(remaining)
  if ARGV[9] and #ARGV[9]>0 and #ARGV[3]>0 and remaining>0 then
    redis.call('SET',key .. ':vector',ARGV[9],'PX',remaining,'NX')
    redis.call('ZADD',KEYS[2],0,'vector-v1:' .. member)
  end
end
-- 有第九个参数就表示这是回填；空字符串表示只有正文。
if ARGV[9] then
  local current = redis.call('GET',key)
  if current then
    if current == ARGV[6] then restore_vector(redis.call('PTTL',key)) end
    return 0
  end
end
-- 淘汰一个回答时也要移除所有旧索引，以免相似检索一直遇到失效候选。
local function discard(m)
  redis.call('ZREM', KEYS[1], m)
  local d, s = string.sub(m,1,64), string.sub(m,66)
  redis.call('DEL', ARGV[1] .. d, ARGV[1] .. d .. ':vector')
  if #s>0 then
    redis.call('ZREM', KEYS[2], s .. ':' .. d, ARGV[8] .. s .. ':' .. d, 'vector-v1:' .. s .. ':' .. d)
  end
end
for _,m in ipairs(redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now)) do discard(m) end
redis.call('SET',key,ARGV[6],'PX',ttl)
redis.call('ZADD',KEYS[1],now+ttl,ARGV[2] .. ':' .. ARGV[3])
if #ARGV[3]>0 then
  local intent = ARGV[8] .. member
  -- 1 表示人工审核过的旧语料，2 表示本地规则识别的意图。
  if ARGV[7]=='1' then
    redis.call('ZADD',KEYS[2],0,member)
  else
    redis.call('ZREM',KEYS[2],member)
  end
  if ARGV[7]=='2' then
    redis.call('ZADD',KEYS[2],0,intent)
  else
    redis.call('ZREM',KEYS[2],intent)
  end
end
restore_vector(ttl)
local extra=redis.call('ZCARD',KEYS[1])-capacity
if extra>0 then
  for _,m in ipairs(redis.call('ZRANGE',KEYS[1],0,extra-1)) do discard(m) end
end
return 1
