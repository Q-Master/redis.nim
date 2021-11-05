import std/[times, options, asyncdispatch]
import ./cmd

#[
  Block of keys commands
    COPY
    *DEL
    *EXISTS
    EXPIRE
    EXPIREAT
    EXPIRETIME
    *KEYS
    MOVE
    PERSIST
    PEXPIRE
    PEXPIREAT
    PEXPIRETIME
    PTTL
    *RANDOMKEY
    *RENAME
    *RENAMENX
    SCAN
    SORT
    *TOUCH
    TTL
    TYPE
    *UNLINK
]#

type
  RedisSortRequest* = ref RedisSortRequestObj
  RedisSortRequestObj* = object of RedisRequestT[RedisStrBool]
    # [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
    by: Option[string]
    limit: Option[Slice[int]]
    gets: seq[string]
    sortOrder: RedisSortOrder
    alpha: bool
  
  RedisSortStoreRequest* = ref object of RedisSortRequest
    store: string

proc newRedisSortRequest(redis: Redis): RedisSortRequest =
  result = newRedisRequest[RedisSortRequest](redis)
  result.gets = @[]
  result.sortOrder = REDIS_SORT_ASC
  result.alpha = false

proc fromRedisSortRequest(req: RedisSortRequest): RedisSortStoreRequest =
  result = newRedisRequest[RedisSortStoreRequest](req.redis)
  result.req = req.req
  result.by = req.by
  result.limit = req.limit
  result.gets = req.gets
  result.sortOrder = req.sortOrder
  result.alpha = req.alpha

# COPY source destination [DB destination-db] [REPLACE] 
proc copy*(redis: Redis, source, destination: string, db: int = -1, replace: bool = false): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("COPY", source, destination)
  if db >= 0:
    result.add(db)
  if replace:
    result.add("REPLACE")

# DEL key [key ...] 
proc del*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("DEL", key)
  if keys.len() > 0:
    result.add(data = keys)

# EXISTS key [key ...] 
proc exists*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("EXISTS", key)
  if keys.len() > 0:
    result.add(data = keys)

# EXPIRE key seconds [NX|XX|GT|LT] 
proc expire*(redis: Redis, key: string, timeout: Duration, expireType: RedisExpireType = REDIS_EXPIRE_NOT_SET): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("EXPIRE", key, timeout.inSeconds)
  if expireType != REDIS_EXPIRE_NOT_SET:
    result.add($expireType)

# EXPIREAT key timestamp [NX|XX|GT|LT] 
proc expireAt*(redis: Redis, key: string, timeout: Time | DateTime, expireType: RedisExpireType = REDIS_EXPIRE_NOT_SET): RedisRequestT[RedisIntBool] =
  var ts: int64
  when timeout is Time:
    ts = timeout.toUnix()
  else:
    ts = timeout.toTime().toUnix()
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("EXPIREAT", key, ts)
  if expireType != REDIS_EXPIRE_NOT_SET:
    result.add($expireType)

# EXPIRETIME key 
proc expireTime*(redis: Redis, key: string): RedisRequestT[Time] =
  result = newRedisRequest[RedisRequestT[Time]](redis)
  result.addCmd("EXPIRETIME", key)

# KEYS pattern 
proc keys*(redis: Redis, pattern: string): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("KEYS", pattern)

# MOVE key db 
proc move*(redis:Redis, key: string, db: int): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("MOVE", key, db)

# PERSIST key 
proc persist*(redis:Redis, key: string): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("PERSIST", key)

# PEXPIRE key milliseconds [NX|XX|GT|LT]
proc pexpire*(redis:Redis, key: string, timeout: Duration, expireType: RedisExpireType = REDIS_EXPIRE_NOT_SET): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("PEXPIRE", key, timeout.inMilliseconds)
  if expireType != REDIS_EXPIRE_NOT_SET:
    result.add($expireType)

# PEXPIREAT key milliseconds-timestamp [NX|XX|GT|LT] 
proc pexpireAt*(redis: Redis, key: string, timeout: Time | DateTime, expireType: RedisExpireType = REDIS_EXPIRE_NOT_SET): RedisRequestT[RedisIntBool] =
  var ts: float
  when timeout is Time:
    ts = timeout.toUnixFloat()
  else:
    ts = timeout.toTime().toUnixFloat()
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("PEXPIREAT", key, (ts*1000).int64)
  if expireType != REDIS_EXPIRE_NOT_SET:
    result.add($expireType)

# PEXPIRETIME key 
proc pexpireTime*(redis: Redis, key: string): RedisRequestT[RedisTimeMillis] =
  result = newRedisRequest[RedisRequestT[RedisTimeMillis]](redis)
  result.addCmd("PEXPIRETIME", key)

# PTTL key 
proc pTTL*(redis: Redis, key: string): RedisRequestT[RedisDurationMillis] =
  result = newRedisRequest[RedisRequestT[RedisDurationMillis]](redis)
  result.addCmd("PTTL", key)

# RANDOMKEY
proc randomKey*(redis: Redis): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("RANDOMKEY")

# RENAME key newkey 
proc rename*(redis: Redis, key: string, newKey: string): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("RENAME", key, newKey)

# RENAMENX key newkey 
proc renameNX*(redis: Redis, key: string, newKey: string): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("RENAMENX", key, newKey)

# SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
proc scan*(redis: Redis, match: Option[string] = string.none, count: int = -1): RedisCursorRequestT[string] =
  result = newRedisCursor[RedisCursorRequestT[string]](redis)
  result.addCmd("SCAN", 0)
  if match.isSome:
    result.add("MATCH", match.get())
  if count > 0:
    result.add("COUNT", count)

# SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
proc realSort[T: RedisRequest](
  req: T, 
  by: Option[string], 
  limit: Option[Slice[int]], 
  gets: seq[string], 
  sortOrder: RedisSortOrder, 
  alpha: bool)

proc sort*(redis: Redis, key: string): RedisSortRequest =
  result = newRedisSortRequest(redis)
  result.addCmd("SORT", key)

proc by*(req: RedisSortRequest, pattern: string): RedisSortRequest =
  result = req
  result.by = pattern.option

proc limit*(req: RedisSortRequest, limit: Slice[int]): RedisSortRequest =
  result = req
  result.limit = limit.option

proc get*(req: RedisSortRequest, pattern: string): RedisSortRequest =
  result = req
  result.gets.add(pattern)

proc order*(req: RedisSortRequest, order: RedisSortOrder): RedisSortRequest =
  result = req
  result.sortOrder = order

proc alpha*(req: RedisSortRequest): RedisSortRequest =
  result = req
  result.alpha = true

proc store*(req: RedisSortRequest, storeKey: string): RedisSortStoreRequest =
  result = req.fromRedisSortRequest()
  result.store = storeKey

proc execute*(req: RedisSortRequest): Future[seq[string]] =
  req.realSort(req.by, req.limit, req.gets, req.sortOrder, req.alpha)
  result = cast[RedisArrayRequestT[string]](req).execute()

proc execute*(req: RedisSortStoreRequest): Future[int64] =
  req.realSort(req.by, req.limit, req.gets, req.sortOrder, req.alpha)
  req.add("STORE", req.store)
  result = cast[RedisRequestT[int64]](req).execute()

# TOUCH key [key ...]
proc touch*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("TOUCH", key)
  if keys.len > 0:
    result.add(data = keys)

# TTL key 
proc ttl*(redis: Redis, key: string): RedisRequestT[Duration] =
  result = newRedisRequest[RedisRequestT[Duration]](redis)
  result.addCmd("TTL", key)

# TYPE key 
proc getType*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("TYPE", key)

# UNLINK key [key ...]
proc unlink*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("UNLINK", key)
  if keys.len > 0:
    result.add(data = keys)

#------- pvt

proc realSort[T: RedisRequest](
  req: T,
  by: Option[string], 
  limit: Option[Slice[int]], 
  gets: seq[string], 
  sortOrder: RedisSortOrder, alpha: bool) =
  if sortOrder == REDIS_SORT_NONE:
    req.add("BY", "nosort")
  elif by.isSome:
    req.add("BY", by.get())
  if limit.isSome:
    let lmt = limit.get()
    req.add("LIMIT", lmt.a, lmt.b)
  if gets.len > 0:
    for get in gets:
      req.add("GET", get)
  if sortOrder == REDIS_SORT_DESC:
    req.add("DESC")
  if alpha:
    req.add("ALPHA")
