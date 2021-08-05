import std/[asyncdispatch, strutils, options]
import ../connection
import ../proto

export connection, proto

type
  RedisRequest* = ref RedisRequestObj
  RedisRequestObj* = object of RootObj
    redis*: Redis
    req*: seq[string]
  RedisBoolRequest* = ref object of RedisRequest
  RedisBoolStrRequest* = ref object of RedisRequest
  RedisBoolIntRequest* = ref object of RedisRequest
  RedisIntRequest* = ref object of RedisRequest
  RedisFloatRequest* = ref object of RedisRequest
  RedisStrRequest* = ref object of RedisRequest
  RedisArrayRequest*[T] = ref object of RedisRequest

proc initRedisRequest*(req: RedisRequest, redis: Redis) =
  req.redis = redis
  req.req = @[]

proc newRedisRequest*(redis: Redis): RedisRequest =
  result.new
  result.initRedisRequest(redis)

proc encodeCommand(cmd: string, args: varargs[RedisMessage, encodeRedis]): RedisMessage

proc add*[T: RedisRequest](req: T, data: RedisMessage): T {.discardable.} =
  result = req
  result.req = result.req & data.prepareRequest()

proc add*[T: RedisRequest](req: T, data: varargs[RedisMessage, encodeRedis]): T {.discardable.} =
  result = req
  for x in data:
    req.add(x)

proc addCmd*[T: RedisRequest](req: T, cmd: string, args: varargs[RedisMessage, encodeRedis]): T {.discardable.} =
  result = req.add(encodeCommand(cmd, args))

proc execute*(req: RedisRequest): Future[RedisMessage] {.async.} =
  await req.redis.sendLine(req.req)
  result = await req.redis.parseResponse()

proc execute*(req: RedisBoolRequest): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.boolean

proc execute*(req: RedisBoolStrRequest): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.str.get("") == "OK")

proc execute*(req: RedisBoolIntRequest): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.integer > 0)

proc execute*(req: RedisIntRequest): Future[int64] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer

proc execute*(req: RedisFloatRequest): Future[float] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.double

proc execute*(req: RedisStrRequest): Future[Option[string]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.str

proc execute*(req: RedisArrayRequest[bool]): Future[seq[bool]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  for x in res.arr:
    result.add(x.boolean)

proc execute*(req: RedisArrayRequest[string]): Future[seq[string]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  for x in res.arr:
    if x.str.isSome:
      result.add(x.str.get())

proc execute*(req: RedisArrayRequest[int64]): Future[seq[int64]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  for x in res.arr:
      result.add(x.integer)

proc execute*(req: RedisArrayRequest[float]): Future[seq[float]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  for x in res.arr:
      result.add(x.double)


proc cmd*(redis: Redis, cmd: string, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] =
  var req = newRedisRequest(redis)
  req.addCmd(cmd, args)
  result = req.execute()

proc next*(cursor: RedisCursor): Future[RedisMessage] {.async.} =
  if cursor.exhausted:
    result = nil
  else:
    let res = await cursor.redis.cmd(cursor.cmd, args = cursor.args)
    if res.kind == REDIS_MESSAGE_ARRAY:
      cursor.updateRedisCursor(res.arr[0].integer)
      result = res.arr[1]
    else:
      cursor.updateRedisCursor(0)
      result = nil

#------- pvt

proc encodeCommand(cmd: string, args: varargs[RedisMessage, encodeRedis]): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_ARRAY)
  for c in cmd.splitWhitespace():
    result.arr.add(c.encodeRedis())
  if args.len > 0:
    for arg in args:
      result.arr.add(arg)