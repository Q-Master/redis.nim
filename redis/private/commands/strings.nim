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

  RedisSetRequest* = ref object of RedisRequest
    ex: bool
    px: bool
    keepttl: bool
    nx: bool
    xx: bool
  
  RedisSetGetRequest* = ref object of RedisSetRequest

proc newRedisGetExRequest(redis: Redis): RedisGetExRequest =
  result.new
  result.initRedisRequest(redis)
  result.ex = false
  result.px = false
  result.persist = false

proc newRedisSetRequest(redis: Redis): RedisSetRequest =
  result.new
  result.initRedisRequest(redis)
  result.ex = false
  result.px = false
  result.keepttl = false
  result.nx = false
  result.xx = false

# APPEND key value 
proc append*(redis: Redis, key, data: string): Future[int64] {.async.} =
  let res = await redis.cmd("APPEND", key, data)
  result = res.integer

# DECR key  
proc decr*(redis: Redis, key: string): Future[int64] {.async.} =
  let res = await redis.cmd("DECR", key)
  result = res.integer

# DECRBY key decrement 
proc decrBy*(redis: Redis, key: string, num: int64): Future[int64] {.async.} =
  let res = await redis.cmd("DECRBY", key, num)
  result = res.integer

# GET key
proc get*(redis: Redis, key: string): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GET", key)
  result = res.str

# GETDEL key
proc getDel*(redis: Redis, key: string): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GETDEL", key)
  result = res.str

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
proc getRange*(redis: Redis, key: string, ranges: Slice[int]): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GETRANGE", key, ranges.a, ranges.b)
  result = res.str

# GETSET key value
proc getSet*(redis: Redis, key, value: string): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GETSET", key, value)
  result = res.str

# INCR key
proc incr*(redis: Redis, key: string): Future[int64] {.async.} =
  let res = await redis.cmd("INCR", key)
  result = res.integer

# INCRBY key increment
proc incrBy*(redis: Redis, key: string, num: int64): Future[int64] {.async.} =
  let res = await redis.cmd("INCRBY", key, num)
  result = res.integer

# INCRBYFLOAT key increment
proc incrBy*(redis: Redis, key: string, num: float): Future[float] {.async.} =
  let res = await redis.cmd("INCRBYFLOAT", key, num)
  result = res.double

# MGET key [key ...] 
proc mget*(redis: Redis, keys: varargs[string, `$`]): Future[seq[Option[string]]] =
  var args: seq[RedisMessage] = @[]
  for k in keys:
    args.add(k.encodeRedis())
  var resFuture = newFuture[seq[Option[string]]]("strings.mget")
  var fut = redis.cmd("MGET", args=args)
  fut.callback =
    proc(future: Future[RedisMessage]) =
      if future.failed:
        resFuture.fail(future.readError())
      else:
        var res: seq[Option[string]] = @[]
        for str in future.read.arr:
          res.add(str.str)
        resFuture.complete(res)
  result = resFuture

proc realMset(redis: Redis, nx: bool, keyvalues: varargs[RedisMessage]): Future[bool]

# MSET key value [key value ...] 
proc mset*(redis: Redis, keyvalues: varargs[RedisMessage, encodeRedis]): Future[bool] =
  result = redis.realMset(false, keyvalues=keyvalues)

# MSETNX key value [key value ...] 
proc msetNx*(redis: Redis, keyvalues: varargs[RedisMessage, encodeRedis]): Future[bool] =
  result = redis.realMset(true, keyvalues=keyvalues)

proc realSetEx[T: Time | DateTime | Duration](redis: Redis, key: string, millis: bool, timeout: T, value: string): Future[bool] {.async.}

# SETEX key seconds value
proc setEx*[T: Time | DateTime | Duration](redis: Redis, key: string, timeout: T, value: string): Future[bool] {.async.} =
  result = await realSetEx[T](redis, key, false, timeout, value)

# PSETEX key milliseconds value
proc psetEx*[T: Time | DateTime | Duration](redis: Redis, key: string, timeout: T, value: string): Future[bool] {.async.} =
  result = await realSetEx[T](redis, key, true, timeout, value)

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

proc execute*(req: RedisSetRequest): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.str.get("") == "OK")

proc execute*(req: RedisSetGetRequest): Future[Option[string]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.str

# SETNX key value 
proc setNx*(redis: Redis, key, value: string): Future[bool] {.async.} =
  let res = await redis.cmd("SETNX", key, value)
  result = res.integer == 1

# SETRANGE key offset value 
proc setRange*(redis: Redis, key, value: string, offset: int64): Future[int64] {.async.} =
  let res = await redis.cmd("SETRANGE", key, offset, value)
  result = res.integer

# STRLEN key
proc strLen*(redis: Redis, key: string): Future[int64] {.async.} =
  let res = await redis.cmd("STRLEN", key)
  result = res.integer

#------- pvt

proc realMset(redis: Redis, nx: bool, keyvalues: varargs[RedisMessage]): Future[bool] =
  var args: seq[RedisMessage] = @[]
  for kv in keyvalues:
    if kv.kind != REDIS_MESSAGE_MAP:
      raise newException(RedisCommandError, "MSET(NX) accepts only {\"a\": b} arguments")
    for k,v in kv.map.pairs:
      args.add(k.encodeRedis())
      args.add(v)
  var resFuture = newFuture[bool]((if nx: "strings.msetNx" else: "strings.mset"))
  var fut: Future[RedisMessage] 
  if nx:
    fut = redis.cmd("MSETNX", args=args)
  else:
    fut = redis.cmd("MSET", args=args)
  fut.callback =
    proc(future: Future[RedisMessage]) =
      if future.failed:
        resFuture.fail(future.readError())
      else:
        if nx:
          resFuture.complete(future.read.integer == 1)
        else:
          resFuture.complete(true)
  result = resFuture

proc realSetEx[T: Time | DateTime | Duration](redis: Redis, key: string, millis: bool, timeout: T, value: string): Future[bool] {.async.} =
  var command = newRedisRequest(redis)
  command.addCmd("PSETEX", key)
  when T is Time or T is DateTime:
    var t: Time
    when T is DateTime:
      t = timeout.toTime()
    else:
      t = timeout
    if millis:
      command.add(int64(t.toUnixFloat()*1000), value)
    else:
      command.add(t.toUnix(), value)
  else:
    if millis:
      command.add(timeout.inMilliseconds, value)
    else:
      command.add(timeout.inSeconds, value)
  let res = await command.execute()
  result = (res.str.get("") == "OK")
