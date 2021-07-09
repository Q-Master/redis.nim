import std/[asyncdispatch, strutils, tables, times, macros]
import ./cmd
#import ../exceptions

#[
  Block of keys commands
    *COPY
    *DEL
    *DUMP
    *EXISTS
    *EXPIRE
    *EXPIREAT
    *EXPIRETIME
    *KEYS
    MIGRATE
    *MOVE
    OBJECT
    PERSIST
    PEXPIRE
    PEXPIREAT
    PEXPIRETIME
    PTTL
    RANDOMKEY
    RENAME
    RENAMENX
    RESTORE
    SCAN
    SORT
    TOUCH
    TTL
    TYPE
    UNLINK
    WAIT
]#

proc copy*(redis: Redis, source, destination: string, db: int = -1, replace: bool = false): Future[bool] {.async.} =
  var args: seq[RedisMessage] = @[]
  args.add(source.encodeRedis())
  args.add(destination.encodeRedis())
  if db >= 0:
    args.add(db.encodeRedis())
  if replace:
    args.add("REPLACE".encodeRedis())
  let res = await redis.cmd("COPY", args = args)
  result = (if res.integer > 0: true else: false)

template del*(redis: Redis, keys: varargs[string, `$`]): untyped =
  block:
    proc realDel(): Future[int] {.async.} =
      var res: RedisMessage
      var args: seq[RedisMessage] = @[]
      for k in keys:
        args.add(k.encodeRedis())
      res = await cmd(redis, "DEL", args=args)
      result = res.integer.int
    realDel()

proc dump*(redis: Redis, key: string): Future[string] {.async.} =
  let res = await redis.cmd("DUMP", key)
  result = res.str[]

template exists*(redis: Redis, keys: varargs[string, `$`]): untyped =
  block:
    proc realExists(): Future[int] {.async.} =
      var res: RedisMessage
      var args: seq[RedisMessage] = @[]
      for k in keys:
        args.add(k.encodeRedis())
      res = await cmd(redis, "EXISTS", args=args)
      result = res.integer.int
    realExists()

proc expire*(redis: Redis, key: string, timeout: int): Future[bool] {.async.} =
  let res = await redis.cmd("EXPIRE", key, timeout)
  result = (res.integer == 1)

proc expireAt*(redis: Redis, key: string, timeout: Time | DateTime): Future[bool] {.async.} =
  var ts: int64
  when timeout is Time:
    ts = timeout.toUnix()
  else:
    ts = timeout.toTime().toUnix()
  let res = await redis.cmd("EXPIREAT", key, ts)
  result = (res.integer == 1)

proc expireTime*(redis: Redis, key: string): Future[Time] {.async.} =
  let res = await redis.cmd("EXPIRETIME", key)
  result = res.integer.fromUnix()

proc keys*(redis: Redis, pattern: string): Future[seq[string]] {.async.} =
  result = @[]
  let res = await redis.cmd("KEYS", pattern)
  for key in res.arr:
    if not key.str.isNil:
      result.add(key.str[])

proc move*(redis:Redis, key: string, db: int): Future[bool] {.async.} =
  let res = await redis.cmd("MOVE", key, db)
  result = (res.integer == 1)
