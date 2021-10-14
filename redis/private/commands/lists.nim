import std/[times, options]
import ./cmd

#[
  Block of list commands
    BLMOVE
    BLPOP
    BRPOP
    BRPOPLPUSH
    LINDEX
    LINSERT
    LLEN
    LMOVE
    LPOP
    LPOS
    LPUSH
    LPUSHX
    LRANGE
    LREM
    LSET
    LTRIM
    RPOP
    RPOPLPUSH
    RPUSH
    RPUSHX
]#
type
  RedisMoveDirection* = enum
    MOVE_LEFT = "LEFT"
    MOVE_RIGHT = "RIGHT"
  RedisIndexSide* = enum
    INDEX_BEFORE = "BEFORE"
    INDEX_AFTER = "AFTER"

# BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
proc blMove*(redis: Redis, timeout: Duration, srcKey: string, srcDirection: RedisMoveDirection, destKey: string, destDirection: RedisMoveDirection): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("BLMOVE", srcKey, destKey, $srcDirection, $destDirection, timeout.inSeconds)

# BLPOP key [key ...] timeout
proc blPop*(redis: Redis, timeout: Duration, keys: varargs[RedisMessage, encodeRedis]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("BLPOP", args=keys)
  result.add(timeout.inSeconds)

# BRPOP key [key ...] timeout 
proc brPop*(redis: Redis, timeout: Duration, keys: varargs[RedisMessage, encodeRedis]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("BRPOP", args=keys)
  result.add(timeout.inSeconds)

# BRPOPLPUSH source destination timeout (deprecated since 6.2)
proc brPoplPush*(redis: Redis, timeout: Duration, srcKey, destKey: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("BRPOPLPUSH", srcKey, destKey, timeout.inSeconds)

# LINDEX key index
proc lIndex*(redis: Redis, key: string, index: int64): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("LINDEX", key, index)

# LINSERT key BEFORE|AFTER pivot element 
proc lInsert*(redis: Redis, key, pivot, element: string, side: RedisIndexSide): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LINSERT", key, $side, pivot, element)

# LLEN key 
proc lLen*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LLEN", key)

# LMOVE source destination LEFT|RIGHT LEFT|RIGHT 
proc lMove*(redis: Redis, srcKey: string, srcDirection: RedisMoveDirection, destKey: string, destDirection: RedisMoveDirection): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("LMOVE", srcKey, destKey, $srcDirection, $destDirection)

# LPOP key [count]
proc lPop*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("LPOP", key)

proc lPop*(redis: Redis, key: string, count: SomeInteger): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("LPOP", key, count.int64)

# LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
proc lPos*(redis: Redis, key, element: string, rank: Option[SomeInteger] = int64.none, maxLen: Option[SomeInteger] = int64.none): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LPOS", key, element)
  if rank.isSome:
    result.add("RANK", rank.get().int64)
  if maxLen.isSome:
    result.add("MAXLEN", maxLen.get().int64)

proc lPos*(redis: Redis, key, element: string, count: SomeInteger, rank: Option[int64] = int64.none, maxLen: Option[int64] = int64.none): RedisArrayRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LPOS", key, element)
  if rank.isSome:
    result.add("RANK", rank.get().int64)
  if maxLen.isSome:
    result.add("MAXLEN", maxLen.get().int64)
  result.add("COUNT", count.int64)

# LPUSH key element [element ...]
proc lPush*(redis: Redis, key: string, elements: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LPUSH", key)
  result.add(data = elements)

# LPUSHX key element [element ...] 
proc lPushX*(redis: Redis, key: string, elements: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LPUSHX", key)
  result.add(data = elements)

# LRANGE key start stop
proc lRange*(redis: Redis, key: string, start, stop: SomeInteger): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("LRANGE", key, start.int64, stop.int64)

# LREM key count element
proc lRem*(redis: Redis, key, element: string, count: SomeInteger): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LREM", key, count.int64, element)

# LSET key index element
proc lSet*(redis: Redis, key, element: string, index: SomeInteger): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("LSET", key, index.int64, element)

# LTRIM key start stop 
proc lTrim*(redis: Redis, key: string, start, stop: SomeInteger): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("LTRIM", key, start.int64, stop.int64)

# RPOP key [count]
proc rPop*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("RPOP", key)

proc rPop*(redis: Redis, key: string, count: SomeInteger): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("RPOP", key, count.int64)

# RPOPLPUSH source destination (deprecated since 6.2)
proc brPoplPush*(redis: Redis, srcKey, destKey: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("RPOPLPUSH", srcKey, destKey)

# RPUSH key element [element ...]
proc rPush*(redis: Redis, key: string, elements: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("RPUSH", key)
  result.add(data = elements)

# RPUSHX key element [element ...]
proc rPushX*(redis: Redis, key: string, elements: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("RPUSHX", key)
  result.add(data = elements)
