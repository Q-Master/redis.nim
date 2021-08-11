import std/[asyncdispatch, strutils, tables, times, macros, options]
import ./cmd
import ../exceptions

#[
  Block of strings commands
    *APPEND
    *DECR
    *DECRBY
    *GET
    *GETDEL
    *GETEX
    *GETRANGE
    *GETSET
    *INCR
    *INCRBY
    *INCRBYFLOAT
    *MGET
    *MSET
    *MSETNX
    *PSETEX
    *SET
    *SETEX
    *SETNX
    *SETRANGE
    STRALGO
    STRLEN
]#
type
  RedisSetOption* = enum
    REDIS_SET_NONE
    REDIS_SET_NX
    REDIS_SET_XX

  RedisGetExRequest* = ref object of RedisRequestT[Option[string]]
    ex: bool
    px: bool
    persist: bool

  RedisSetRequest* = ref object of RedisRequestT[RedisStrBool]
    ex: bool
    px: bool
    keepttl: bool
    nx: bool
    xx: bool
  
  RedisSetGetRequest* = ref object of RedisSetRequest

proc newRedisGetExRequest(redis: Redis): RedisGetExRequest =
  result = newRedisRequest[RedisGetExRequest](redis)
  result.ex = false
  result.px = false
  result.persist = false

proc newRedisSetRequest(redis: Redis): RedisSetRequest =
  result = newRedisRequest[RedisSetRequest](redis)
  result.ex = false
  result.px = false
  result.keepttl = false
  result.nx = false
  result.xx = false

# APPEND key value 
proc append*(redis: Redis, key, data: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("APPEND", key, data)

# DECR key  
proc decr*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("DECR", key)

# DECRBY key decrement 
proc decrBy*(redis: Redis, key: string, num: int64): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("DECRBY", key, num)

# GET key
proc get*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("GET", key)

# GETDEL key
proc getDel*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("GETDEL", key)

# GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|PERSIST]
proc getEx*(redis: Redis, key: string): RedisGetExRequest =
  result = newRedisGetExRequest(redis)
  result.addCmd("GETEX", key)

proc ex*(req: RedisGetExRequest, t: Duration): RedisGetExRequest =
  result = req
  if result.persist:
    raise newException(RedisConnectionError, "Conflicting options for GETEX PERSIST vs EX")
  if result.px:
    raise newException(RedisConnectionError, "Conflicting options for GETEX PX vs EX")
  result.ex = true
  result.add("EX", t.inSeconds)

proc px*(req: RedisGetExRequest, t: Duration): RedisGetExRequest =
  result = req
  if result.persist:
    raise newException(RedisConnectionError, "Conflicting options for GETEX PERSIST vs PX")
  if result.ex:
    raise newException(RedisConnectionError, "Conflicting options for GETEX EX vs PX")
  result.px = true
  result.add("PX", t.inMilliseconds)

proc exAt*[T: Time | DateTime](req: RedisGetExRequest, t: T): RedisGetExRequest =
  result = req
  if result.persist:
    raise newException(RedisConnectionError, "Conflicting options for GETEX PERSIST vs EXAT")
  if result.px:
    raise newException(RedisConnectionError, "Conflicting options for GETEX PXAT vs EXAT")
  var tt: Time
  when T is DateTime:
    tt = t.toTime()
  else:
    tt = t
  result.ex = true
  result.add("EXAT", tt.toUnix())

proc pxAt*[T: Time | DateTime](req: RedisGetExRequest, t: T): RedisGetExRequest =
  result = req
  if result.persist:
    raise newException(RedisConnectionError, "Conflicting options for GETEX PERSIST vs EXAT")
  if result.px:
    raise newException(RedisConnectionError, "Conflicting options for GETEX PXAT vs EXAT")
  var tt: Time
  when T is DateTime:
    tt = t.toTime()
  else:
    tt = t
  result.px = true
  result.add("PXAT", int64(tt.toUnixFloat()*1000))

proc persist*(req: RedisGetExRequest): RedisGetExRequest =
  result = req
  if result.ex or result.px:
    raise newException(RedisConnectionError, "Conflicting options for GETEX EX/PX vs PERSIST")
  result.persist = true
  result.add("PERSIST")

# GETRANGE key start end 
proc getRange*(redis: Redis, key: string, ranges: Slice[int]): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("GETRANGE", key, ranges.a, ranges.b)

# GETSET key value
proc getSet*(redis: Redis, key, value: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("GETSET", key, value)

# INCR key
proc incr*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("INCR", key)

# INCRBY key increment
proc incrBy*(redis: Redis, key: string, num: int64): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("INCRBY", key, num)

# INCRBYFLOAT key increment
proc incrBy*(redis: Redis, key: string, num: float): RedisRequestT[float] =
  result = newRedisRequest[RedisRequestT[float]](redis)
  result.addCmd("INCRBYFLOAT", key, num)

# MGET key [key ...] 
proc mGet*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisArrayRequest[Option[string]] =
  result = newRedisRequest[RedisArrayRequest[Option[string]]](redis)
  result.addCmd("MGET", key)
  if keys.len > 0:
    result.add(data = keys)

# MSET key value [key value ...] 
proc mSet*[T](redis: Redis, keyValue: tuple[a: string, b: T], keyValues: varargs[tuple[a: string, b: T]]): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("MSET", keyValue)
  for kv in keyValues:
    result.add(kv.a, kv.b)

# MSETNX key value [key value ...] 
proc mSetNX*[T](redis: Redis, keyValue: tuple[a: string, b: T], keyValues: varargs[tuple[a: string, b: T]]): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("MSETNX", keyValue.a, keyValue.b)
  for kv in keyValues:
    result.add(kv.a, kv.b)

# SETEX key seconds value
proc setEx*[T: Time | DateTime | Duration](redis: Redis, key: string, value: string, timeout: T): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("SETEX", key)
  when T is Time or T is DateTime:
    var t: Time
    when T is DateTime:
      t = timeout.toTime()
    else:
      t = timeout
    result.add(t.toUnix(), value)
  else:
    result.add(timeout.inSeconds, value)

# PSETEX key milliseconds value
proc pSetEx*[T: Time | DateTime | Duration](redis: Redis, key: string, value: string, timeout: T): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("PSETEX", key)
  when T is Time or T is DateTime:
    var t: Time
    when T is DateTime:
      t = timeout.toTime()
    else:
      t = timeout
    result.add(int64(t.toUnixFloat()*1000), value)
  else:
    result.add(timeout.inMilliseconds, value)

# SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
proc setVal*(redis: Redis, key, value: string): RedisSetRequest =
  result = newRedisSetRequest(redis)
  result.addCmd("SET", key, value)

proc ex*(req: RedisSetRequest, t: Duration): RedisSetRequest =
  result = req
  if result.keepttl:
    raise newException(RedisConnectionError, "Conflicting options for SET KEEPTTL vs EX")
  if result.px:
    raise newException(RedisConnectionError, "Conflicting options for SET PX vs EX")
  result.ex = true
  result.add("EX", t.inSeconds)

proc px*(req: RedisSetRequest, t: Duration): RedisSetRequest =
  result = req
  if result.keepttl:
    raise newException(RedisConnectionError, "Conflicting options for SET KEEPTTL vs EX")
  if result.ex:
    raise newException(RedisConnectionError, "Conflicting options for SET EX vs PX")
  result.px = true
  result.add("PX", t.inMilliseconds)

proc exAt*[T: Time | DateTime](req: RedisSetRequest, t: T): RedisSetRequest =
  result = req
  if result.keepttl:
    raise newException(RedisConnectionError, "Conflicting options for SET KEEPTTL vs EXAT")
  if result.px:
    raise newException(RedisConnectionError, "Conflicting options for SET PXAT vs EXAT")
  var tt: Time
  when T is DateTime:
    tt = t.toTime()
  else:
    tt = t
  result.ex = true
  result.add("EXAT", t.toUnix())

proc pxAt*[T: Time | DateTime](req: RedisSetRequest, t: T): RedisSetRequest =
  result = req
  if result.keepttl:
    raise newException(RedisConnectionError, "Conflicting options for SET KEEPTTL vs EXAT")
  if result.px:
    raise newException(RedisConnectionError, "Conflicting options for SET PXAT vs EXAT")
  var tt: Time
  when T is DateTime:
    tt = t.toTime()
  else:
    tt = t
  result.px = true
  result.add("EXAT", int64(t.toUnixFloat()*1000))

proc keepTTL*(req: RedisSetRequest): RedisSetRequest =
  result = req
  if result.px or result.ex:
    raise newException(RedisConnectionError, "Conflicting options for SET EX/PX vs KEEPTTL")
  result.keepttl = true
  result.add("KEEPTTL")

proc nx*(req: RedisSetRequest): RedisSetRequest =
  result = req
  if result.xx:
    raise newException(RedisConnectionError, "Conflicting options for SET XX vs NX")
  result.nx = true
  result.add("NX")

proc xx*(req: RedisSetRequest): RedisSetRequest =
  result = req
  if result.nx:
    raise newException(RedisConnectionError, "Conflicting options for SET NX vs XX")
  result.xx = true
  result.add("XX")

proc get*(req: RedisSetRequest): RedisSetGetRequest =
  result = cast[RedisSetGetRequest](req)
  result.add("GET")

proc execute*(req: RedisSetGetRequest): Future[Option[string]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.str

# SETNX key value 
proc setNx*(redis: Redis, key, value: string): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("SETNX", key, value)

# SETRANGE key offset value 
proc setRange*(redis: Redis, key, value: string, offset: int64): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SETRANGE", key, offset, value)

# STRLEN key
proc strLen*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("STRLEN", key)
