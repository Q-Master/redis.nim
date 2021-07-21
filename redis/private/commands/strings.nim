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
    MSETNX
    PSETEX
    SET
    SETEX
    SETNX
    SETRANGE
    STRALGO
    STRLEN
]#

proc append*(redis: Redis, key, data: string): Future[int64] {.async.} =
  let res = await redis.cmd("APPEND", key, data)
  result = res.integer

proc decr*(redis: Redis, key: string): Future[int64] {.async.} =
  let res = await redis.cmd("DECR", key)
  result = res.integer

proc decrBy*(redis: Redis, key: string, num: int64): Future[int64] {.async.} =
  let res = await redis.cmd("DECRBY", key, num)
  result = res.integer

proc get*(redis: Redis, key: string): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GET", key)
  result = res.str

proc getDel*(redis: Redis, key: string): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GETDEL", key)
  result = res.str

proc realGetEx[T: Time | DateTime | Duration](redis: Redis, key: string, millis: bool, timeout: Option[T] = Time.none, persist = false): Future[Option[string]] {.async.}

proc getEx*[T: Time | DateTime | Duration](redis: Redis, key: string, timeout: Option[T] = Time.none, persist = false): Future[Option[string]] {.async.} =
  result = await realGetEx[T](redis, key, false, timeout=timeout, persist=persist)

proc getPEx*[T: Time | DateTime | Duration](redis: Redis, key: string, timeout: Option[T] = Time.none, persist = false): Future[Option[string]] {.async.} =
  result = await realGetEx[T](redis, key, true, timeout=timeout, persist=persist)

proc getRange*(redis: Redis, key: string, ranges: Slice[int]): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GETRANGE", key, ranges.a, ranges.b)
  result = res.str

proc getSet*(redis: Redis, key, data: string): Future[Option[string]] {.async.} =
  let res = await redis.cmd("GETSET", key, data)
  result = res.str

proc incr*(redis: Redis, key: string): Future[int64] {.async.} =
  let res = await redis.cmd("INCR", key)
  result = res.integer

proc incrBy*(redis: Redis, key: string, num: int64): Future[int64] {.async.} =
  let res = await redis.cmd("INCRBY", key, num)
  result = res.integer

proc incrBy*(redis: Redis, key: string, num: float): Future[float] {.async.} =
  let res = await redis.cmd("INCRBYFLOAT", key, num)
  result = res.double

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

proc mset*(redis: Redis, keyvalues: varargs[RedisMessage, encodeRedis]): Future[bool] =
  var args: seq[RedisMessage] = @[]
  for kv in keyvalues:
    if kv.kind != REDIS_MESSAGE_MAP:
      raise newException(RedisCommandError, "MSET accepts only {\"a\": b} arguments")
    for k,v in kv.map.pairs:
      args.add(k.encodeRedis())
      args.add(v)
  var resFuture = newFuture[bool]("strings.mset")
  var fut = redis.cmd("MSET", args=args)
  fut.callback =
    proc(future: Future[RedisMessage]) =
      if future.failed:
        resFuture.fail(future.readError())
      else:
        resFuture.complete(true)
  result = resFuture

#------- pvt

proc realGetEx[T: Time | DateTime | Duration](redis: Redis, key: string, millis: bool, timeout: Option[T] = Time.none, persist = false): Future[Option[string]] {.async.} =
  var res: RedisMessage
  if persist:
    res = await redis.cmd("GETEX", key, "PERSIST")
  else:
    if timeout.isSome:
      let realTimeout: T = timeout.get()
      when T is Time or T is DateTime:
        var t: Time
        when T is DateTime:
          t = realTimeout.toTime()
        else:
          t = realTimeout
        if millis:
          res = await redis.cmd("GETEX", key, "PEXAT", int64(t.toUnixFloat()*1000))
        else:
          res = await redis.cmd("GETEX", key, "EXAT", t.toUnix())
      else:
        if millis:
          res = await redis.cmd("GETEX", key, "PEX", realTimeout.inMilliseconds)
        else:
          res = await redis.cmd("GETEX", key, "EX", realTimeout.inSeconds)
    else:
      res = await redis.cmd("GETEX", key)
  result = res.str