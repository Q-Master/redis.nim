import std/[asyncdispatch, strutils, tables, times, macros, options]
import ./cmd
import ../exceptions

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
    *PERSIST
    *PEXPIRE
    *PEXPIREAT
    *PEXPIRETIME
    *PTTL
    *RANDOMKEY
    *RENAME
    *RENAMENX
    RESTORE
    *SCAN
    *SORT
    *TOUCH
    *TTL
    TYPE
    *UNLINK
    WAIT
]#

type
  RedisScanCursor* = ref object of RedisCursor
    scanReply: seq[RedisMessage]

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
  result = res.str.get("")

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

proc expire*(redis: Redis, key: string, timeout: Duration): Future[bool] {.async.} =
  let res = await redis.cmd("EXPIRE", key, timeout.inSeconds)
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
    if key.str.isSome:
      result.add(key.str.get())

proc move*(redis:Redis, key: string, db: int): Future[bool] {.async.} =
  let res = await redis.cmd("MOVE", key, db)
  result = (res.integer == 1)

proc persist*(redis:Redis, key: string): Future[bool] {.async.} =
  let res = await redis.cmd("PERSIST", key)
  result = (res.integer == 1)

proc pexpire*(redis:Redis, key: string, timeout: Duration): Future[bool] {.async.} =
  let res = await redis.cmd("PEXPIRE", key, timeout.inMilliseconds)
  result = (res.integer == 1)

proc pexpireAt*(redis: Redis, key: string, timeout: Time | DateTime): Future[bool] {.async.} =
  var ts: float
  when timeout is Time:
    ts = timeout.toUnixFloat()
  else:
    ts = timeout.toTime().toUnixFloat()
  let res = await redis.cmd("PEXPIREAT", key, (ts*1000).int64)
  result = (res.integer == 1)

proc pexpireTime*(redis: Redis, key: string): Future[Time] {.async.} =
  let res = await redis.cmd("PEXPIRETIME", key)
  result = (res.integer.float/1000).fromUnixFloat()

proc pTTL*(redis: Redis, key: string): Future[Duration] {.async.} =
  let res = await redis.cmd("PTTL", key)
  if res.integer == -1:
    raise newException(RedisKeyDoesntExist, "Key " & key & " doesn't exist")
  elif res.integer == -2:
    raise newException(RedisKeyDoesntExpire, "Key " & key & " can't expire")
  result = initDuration(milliseconds = res.integer)

proc randomKey*(redis: Redis): Future[Option[string]] {.async.} =
  let res = await redis.cmd("RANDOMKEY")
  result = res.str

proc rename*(redis: Redis, key: string, newKey: string): Future[bool] {.async.} =
  let res = await redis.cmd("RENAME", key, newKey)
  result = res.str.get("") == "OK"

proc renameNX*(redis: Redis, key: string, newKey: string): Future[bool] {.async.} =
  let res = await redis.cmd("RENAMENX", key, newKey)
  result = res.integer == 1

proc scan*(redis: Redis, match: Option[string] = string.none, count: int = -1): RedisScanCursor =
  #  SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
  var args: seq[RedisMessage] = @[]
  if match.isSome:
    args.add("MATCH".encodeRedis())
    args.add(match.get().encodeRedis())
  if count > 0:
    args.add("COUNT".encodeRedis())
    args.add(count.encodeRedis())
  result = newRedisCursor[RedisScanCursor](redis, "SCAN", args=args)
  result.scanReply = @[]

proc next*(cursor: RedisScanCursor): Future[tuple[running: bool, result: string]] {.async.} =
  if cursor.scanReply.len == 0 and not cursor.exhausted:
    let repl = await cast[RedisCursor](cursor).next()
    if repl.isNil:
      cursor.scanReply = @[]
    else:
      cursor.scanReply = repl.arr
  if cursor.scanReply.len == 0:
    result = (false, "")
  else:
    result = (true, cursor.scanReply.pop().str.get())

proc realSort(redis: Redis, key: string, storeKey = string.none, by = string.none, limit = Slice[int].none, sortOrder = REDIS_SORT_ASC, alpha = false, gets: seq[string] = @[]): Future[RedisMessage] {.async.}

proc sort*(redis: Redis, key: string, by = string.none, limit = Slice[int].none, sortOrder = REDIS_SORT_ASC, alpha = false, gets: seq[string] = @[]): Future[seq[string]] {.async.} =
  result = @[]
  let res = await redis.realSort(key, by=by, limit=limit, sortOrder=sortOrder, alpha=alpha, gets=gets)
  for r in res.arr:
    result.add(r.str.get())

proc sortAndStore*(redis: Redis, key: string, storeKey: string, by = string.none, limit = Slice[int].none, sortOrder = REDIS_SORT_ASC, alpha = false, gets: seq[string] = @[]): Future[int64] {.async.} =
  let res = await redis.realSort(key, storeKey=storeKey.option, by=by, limit=limit, sortOrder=sortOrder, alpha=alpha, gets=gets)
  result = res.integer

template touch*(redis: Redis, keys: varargs[string, `$`]): untyped =
  block:
    proc realTouch(): Future[int] {.async.} =
      var res: RedisMessage
      var args: seq[RedisMessage] = @[]
      for k in keys:
        args.add(k.encodeRedis())
      res = await cmd(redis, "TOUCH", args=args)
      result = res.integer.int
    realTouch()

proc ttl*(redis: Redis, key: string): Future[Duration] {.async.} =
  let res = await redis.cmd("TTL", key)
  if res.integer == -1:
    raise newException(RedisKeyDoesntExist, "Key " & key & " doesn't exist")
  elif res.integer == -2:
    raise newException(RedisKeyDoesntExpire, "Key " & key & " can't expire")
  result = initDuration(seconds = res.integer)

template unlink*(redis: Redis, keys: varargs[string, `$`]): untyped =
  block:
    proc realUnlink(): Future[int] {.async.} =
      var res: RedisMessage
      var args: seq[RedisMessage] = @[]
      for k in keys:
        args.add(k.encodeRedis())
      res = await cmd(redis, "UNLINK", args=args)
      result = res.integer.int
    realUnlink()

#------- pvt

proc realSort(redis: Redis, key: string, storeKey = string.none, by = string.none, limit = Slice[int].none, sortOrder = REDIS_SORT_ASC, alpha = false, gets: seq[string] = @[]): Future[RedisMessage] {.async.} =
  var args: seq[RedisMessage]
  args.add(key.encodeRedis())
  if by.isSome:
    args.add("BY".encodeRedis())
    args.add(by.get().encodeRedis())
  if limit.isSome:
    let lmt = limit.get()
    args.add("LIMIT".encodeRedis())
    args.add(lmt.a.encodeRedis())
    args.add(lmt.b.encodeRedis())
  for g in gets:
    args.add("GET".encodeRedis())
    args.add(g.encodeRedis())
  case sortOrder
  of REDIS_SORT_ASC:
    discard
  of REDIS_SORT_DESC:
    args.add("DESC".encodeRedis())
  of REDIS_SORT_NONE:
    if by.isNone:
      args.add("BY".encodeRedis())
      args.add("nosort".encodeRedis())
  if alpha:
    args.add("ALPHA".encodeRedis())
  if storeKey.isSome:
    args.add("STORE".encodeRedis())
    args.add(storeKey.get().encodeRedis())
  result = await redis.cmd("SORT", args=args)
