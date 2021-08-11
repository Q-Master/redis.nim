import std/[asyncdispatch, strutils, options, times]
import ../connection
import ../proto
import ../exceptions

export connection, proto

type
  RedisRequest* = ref RedisRequestObj
  RedisRequestObj* = object of RootObj
    redis*: Redis
    req*: RedisMessage

  RedisStrBool* = distinct bool
  RedisIntBool* = distinct bool
  RedisTimeMillis* = object
  RedisDurationMillis* = object

  RedisRequestT*[T] = ref object of RedisRequest
  RedisArrayRequest*[T] = ref object of RedisRequest
  RedisCursorRequest* = ref object of RedisRequest
    exhausted: bool
    reply: RedisMessage

proc initRedisRequest*(req: RedisRequest, redis: Redis) =
  req.redis = redis
  req.req = nil

proc newRedisRequest*[T: RedisRequest](redis: Redis): T =
  result.new
  result.initRedisRequest(redis)

proc resetRedisCursor*(cursor: RedisCursorRequest) =
  cursor.exhausted = false
  cursor.req.arr[1] = 0.encodeRedis()

proc newRedisCursor*(redis: Redis): RedisCursorRequest =
  result.new
  result.initRedisRequest(redis)
  result.resetRedisCursor()

proc updateRedisCursor(cursor: RedisCursorRequest, id: int64)
proc encodeCommand(cmd: string, args: varargs[RedisMessage, encodeRedis]): RedisMessage

proc add*[T: RedisRequest](req: T, data: RedisMessage): T {.discardable.} =
  result = req
  if req.req.isNil():
    req.req = RedisMessage(kind: REDIS_MESSAGE_ARRAY)
  result.req.arr.add(data)

proc add*[T: RedisRequest](req: T, data: varargs[RedisMessage, encodeRedis]): T {.discardable.} =
  result = req
  for x in data:
    req.add(x)

proc addCmd*[T: RedisRequest](req: T, cmd: string, args: varargs[RedisMessage, encodeRedis]): T {.discardable.} =
  result.req = encodeCommand(cmd, args)

proc execute*(req: RedisRequest): Future[RedisMessage] {.async.} =
  await req.redis.sendLine(req.req.prepareRequest())
  result = await req.redis.parseResponse()

proc execute*(req: RedisRequestT[bool]): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.boolean

proc execute*(req: RedisRequestT[RedisStrBool]): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.str.get("") == "OK")

proc execute*(req: RedisRequestT[RedisIntBool]): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.integer > 0)

proc execute*[T: SomeInteger](req: RedisRequestT[T]): Future[T] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = T(res.integer)

proc execute*(req: RedisRequestT[float]): Future[float] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.double

proc execute*(req: RedisRequestT[Option[string]]): Future[Option[string]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = string.none
  else:
    result = res.str

proc execute*(req: RedisRequestT[string]): Future[string] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.str.get("")

proc execute*(req: RedisRequestT[Time]): Future[Time] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer.fromUnix()

proc execute*(req: RedisRequestT[RedisTimeMillis]): Future[Time] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.integer.float/1000).fromUnixFloat()

proc execute*(req: RedisRequestT[Duration]): Future[Duration] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.integer == -1:
    raise newException(RedisKeyDoesntExist, "Key doesn't exist")
  elif res.integer == -2:
    raise newException(RedisKeyDoesntExpire, "Key can't expire")
  result = initDuration(seconds = res.integer)

proc execute*(req: RedisRequestT[RedisDurationMillis]): Future[Duration] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.integer == -1:
    raise newException(RedisKeyDoesntExist, "Key doesn't exist")
  elif res.integer == -2:
    raise newException(RedisKeyDoesntExpire, "Key can't expire")
  result = initDuration(milliseconds = res.integer)

proc execute*[T](req: RedisRequestT[T]): Future[T] {.async.} =
  mixin fromRedisReq
  let res = await cast[RedisRequest](req).execute()
  result = fromRedisReq(res)

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

proc execute*(req: RedisArrayRequest[RedisIntBool]): Future[seq[bool]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  for x in res.arr:
      result.add(x.integer > 0)

proc execute*(req: RedisArrayRequest[RedisStrBool]): Future[seq[bool]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  for x in res.arr:
      result.add(x.str.get("") == "OK")

proc execute*[T](req: RedisArrayRequest[T]): Future[seq[T]] {.async.} =
  mixin fromRedisReq
  let res = await cast[RedisRequest](req).execute()
  result = fromRedisReq(res)

proc execute*(req: RedisCursorRequest): Future[RedisMessage] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  req.updateRedisCursor(res.arr[0].integer)
  result = res.arr[1]

proc cmd*(redis: Redis, cmd: string, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] =
  var req = newRedisRequest[RedisRequest](redis)
  req.addCmd(cmd, args)
  result = req.execute()

proc next*(cursor: RedisCursorRequest): Future[tuple[stop: bool, res: string]] {.async.} =
  if cursor.reply.arr.len == 0 and not cursor.exhausted:
    cursor.reply = await cursor.execute()
  if cursor.reply.arr.len == 0:
    cursor.exhausted = true
    result = (true, "")
  else:
    result = (false, cursor.reply.arr.pop().str.get())

#------- pvt

proc encodeCommand(cmd: string, args: varargs[RedisMessage, encodeRedis]): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_ARRAY)
  for c in cmd.splitWhitespace():
    result.arr.add(c.encodeRedis())
  if args.len > 0:
    result.arr = result.arr & @args

proc updateRedisCursor(cursor: RedisCursorRequest, id: int64) =
  if id == 0:
    cursor.exhausted = true
  cursor.req.arr[1] = id.encodeRedis()
