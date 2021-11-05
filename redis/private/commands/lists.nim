import std/[times, options, asyncdispatch]
import ./cmd

#[
  Block of list commands
    BLMOVE
    BLPOP
    BRPOP
    BRPOPLPUSH
    *LINDEX
    *LINSERT
    *LLEN
    *LMOVE
    *LPOP
    *LPOS
    *LPUSH
    *LPUSHX
    *LRANGE
    *LREM
    *LSET
    *LTRIM
    *RPOP
    *RPOPLPUSH
    *RPUSH
    *RPUSHX
]#
type
  RedisMoveDirection* = enum
    MOVE_LEFT = "LEFT"
    MOVE_RIGHT = "RIGHT"
  RedisIndexSide* = enum
    INDEX_BEFORE = "BEFORE"
    INDEX_AFTER = "AFTER"

  RedisListPosRequest* = ref RedisListPosRequestObj
  RedisListPosRequestObj* = object of RedisRequestT[int64]
    rank: Option[string]
    maxLen: Option[string]
  
  RedisListPosCountRequest* = ref object of RedisListPosRequest
    count: string

proc newRedisListPosRequest(redis: Redis): RedisListPosRequest =
  result = newRedisRequest[RedisListPosRequest](redis)

proc fromRedisListPosRequest(req: RedisListPosRequest): RedisListPosCountRequest =
  result = newRedisRequest[RedisListPosCountRequest](req.redis)
  result.req = req.req
  result.rank = req.rank
  result.maxLen = req.maxLen

# BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
proc blMove*(redis: Redis, timeout: Duration, srcKey: string, srcDirection: RedisMoveDirection, destKey: string, destDirection: RedisMoveDirection): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("BLMOVE", srcKey, destKey, $srcDirection, $destDirection, timeout.inSeconds)

# BLPOP key [key ...] timeout
proc blPop*(redis: Redis, timeout: Duration, key: string, keys: varargs[string]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("BLPOP", key)
  for elem in keys:
    result.add(elem)
  result.add(timeout.inSeconds)

# BRPOP key [key ...] timeout 
proc brPop*(redis: Redis, timeout: Duration, key: string, keys: varargs[string]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("BRPOP", key)
  for elem in keys:
    result.add(elem)
  result.add(timeout.inSeconds)

# BRPOPLPUSH source destination timeout (deprecated since 6.2)
proc brPoplPush*(redis: Redis, timeout: Duration, srcKey, destKey: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("BRPOPLPUSH", srcKey, destKey, timeout.inSeconds)

# LINDEX key index
proc lIndex*(redis: Redis, key: string, index: int64): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("LINDEX", key, $index)

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
  result.addCmd("LPOP", key, $count)

# LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
proc genlPos(req: RedisListPosRequest | RedisListPosCountRequest)

proc lPos*(redis: Redis, key, element: string): RedisListPosRequest =
  result = newRedisListPosRequest(redis)
  result.addCmd("LPOS", key, element)

proc rank*[T: RedisListPosRequest](req: T, rank: SomeInteger): T =
  result = req
  result.rank = option($rank)

proc maxLen*[T: RedisListPosRequest](req: T, maxLen: SomeInteger): T =
  result = req
  result.maxLen = option($maxLen)

proc count*(req: RedisListPosRequest, count: SomeInteger): RedisListPosCountRequest =
  result = fromRedisListPosRequest(req)
  result.count = $count

proc count*(req: RedisListPosCountRequest, count: SomeInteger): RedisListPosCountRequest =
  result = req
  result.count = $count

proc execute*(req: RedisListPosRequest): Future[Option[int64]] =
  req.genlPos()
  result = cast[RedisRequestT[Option[int64]]](req).execute()

proc execute*(req: RedisListPosCountRequest): Future[seq[int64]] =
  req.genlPos()
  req.add("COUNT", req.count)
  result = cast[RedisArrayRequestT[int64]](req).execute()  

# LPUSH key element [element ...]
proc lPush*(redis: Redis, key: string, elements: varargs[string, `$`]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LPUSH", key)
  for element in elements:
    result.add(element)

# LPUSHX key element [element ...] 
proc lPushX*(redis: Redis, key: string, elements: varargs[string, `$`]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LPUSHX", key)
  for element in elements:
    result.add(element)

# LRANGE key start stop
proc lRange*(redis: Redis, key: string, ranges: Slice[int]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("LRANGE", key, $ranges.a, $ranges.b)

# LREM key count element
proc lRem*(redis: Redis, key, element: string, count: SomeInteger = 0): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("LREM", key, $count, element)

# LSET key index element
proc lSet*(redis: Redis, key, element: string, index: SomeInteger): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("LSET", key, $index, element)

# LTRIM key start stop 
proc lTrim*(redis: Redis, key: string, ranges: Slice[int]): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("LTRIM", key, $ranges.a, $ranges.b)

# RPOP key [count]
proc rPop*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("RPOP", key)

proc rPop*(redis: Redis, key: string, count: SomeInteger): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("RPOP", key, $count)

# RPOPLPUSH source destination (deprecated since 6.2)
proc rPoplPush*(redis: Redis, srcKey, destKey: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("RPOPLPUSH", srcKey, destKey)

# RPUSH key element [element ...]
proc rPush*(redis: Redis, key: string, elements: varargs[string, `$`]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("RPUSH", key)
  for element in elements:
    result.add(element)

# RPUSHX key element [element ...]
proc rPushX*(redis: Redis, key: string, elements: varargs[string, `$`]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("RPUSHX", key)
  for element in elements:
    result.add(element)

#------- pvt

proc genlPos(req: RedisListPosRequest | RedisListPosCountRequest) =
  if req.rank.isSome:
    req.add("RANK", req.rank.get())
  if req.maxLen.isSome:
    req.add("MAXLEN", req.maxLen.get())
