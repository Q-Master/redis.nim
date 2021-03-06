import std/[asyncdispatch, strutils, options, times, sha1]
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
  RedisTimeMillis* = object of RootObj
  RedisDurationMillis* = object of RootObj
  RedisStrFloat* = distinct float
  RedisStrInt* = distinct int64

  RedisRequestT*[T] = ref object of RedisRequest
    val: T
  RedisArrayRequestT*[T] = ref object of RedisRequest
    val: T
  RedisCursorRequest* = ref object of RedisRequest
    exhausted: bool
    reply: RedisMessage
  RedisCursorRequestT*[T] = ref object of RedisCursorRequest

proc initRedisRequest*(req: RedisRequest, redis: Redis) =
  req.redis = redis
  req.req = nil

proc newRedisRequest*[T: RedisRequest](redis: Redis): T =
  result.new
  result.initRedisRequest(redis)

proc newFromRedisRequest*[T](req: RedisRequest): T =
  result = newRedisRequest[T](req.redis)
  result.req = req.req

proc resetRedisCursor*[T: RedisCursorRequest](cursor: T) =
  cursor.exhausted = false
  cursor.req.arr[1] = 0.encodeRedis()

proc newRedisCursor*[T: RedisCursorRequest](redis: Redis): T =
  result.new
  result.initRedisRequest(redis)
  result.resetRedisCursor()

proc updateRedisCursor(cursor: RedisCursorRequest, id: int64)
proc encodeCommand(cmd: string, args: varargs[RedisMessage, encodeRedis]): RedisMessage

proc add*[T: RedisRequest](req: T, data: RedisMessage): T {.discardable.} =
  result = req
  if result.req.isNil():
    result.req = RedisMessage(kind: REDIS_MESSAGE_ARRAY)
  result.req.arr.add(data)

proc add*[T: RedisRequest](req: T, data: varargs[RedisMessage, encodeRedis]): T {.discardable.} =
  result = req
  for x in data:
    result.add(x)

proc insert*[T: RedisRequest](req: T, pos: Natural, data: varargs[RedisMessage, encodeRedis]): T {.discardable.} =
  result = req
  var i = pos
  if req.req.isNil():
    raise newException(RedisError, "Can't insert to an empty array")
  if req.req.arr.len < pos:
    req.add(data=data)
  else:
    for d in data:
      req.req.arr.insert(d, i)
      i.inc(1)

proc extend*[T: RedisRequest, V](req: T, arr: openArray[V]): T {.discardable.} =
  result = req
  for elem in arr:
    result.add(elem)

proc extend*[T: RedisRequest, V, D](req: T, arr: openArray[V], conv: proc(x: V): D): T {.discardable.} =
  result = req
  for elem in arr:
    result.add(conv(elem))

proc addCmd*[T: RedisRequest](req: T, cmd: string, args: varargs[RedisMessage, encodeRedis]): T {.discardable.} =
  result = req
  result.req = encodeCommand(cmd, args)

proc execute*(req: RedisRequest): Future[RedisMessage] {.async.} =
  let data = req.req.prepareRequest()
  req.redis.withRedis:
    await redis.sendLine(data)
    result = await redis.parseResponse()

proc execute*(req: RedisRequestT[bool]): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.boolean

proc execute*(req: RedisRequestT[RedisStrBool]): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.str.get("") == "OK")

proc execute*(req: RedisRequestT[RedisIntBool]): Future[bool] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = (res.integer > 0)

proc execute*(req: RedisRequestT[int64]): Future[int64] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer

proc execute*(req: RedisRequestT[uint64]): Future[uint64] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer.uint64

proc execute*(req: RedisRequestT[int32]): Future[int32] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer.int32

proc execute*(req: RedisRequestT[uint32]): Future[uint32] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer.uint32

proc execute*(req: RedisRequestT[int16]): Future[int16] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer.int16

proc execute*(req: RedisRequestT[uint16]): Future[uint16] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer.uint16

proc execute*(req: RedisRequestT[Option[int64]]): Future[Option[int64]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind in [REDIS_MESSAGE_NIL, REDIS_MESSAGE_STRING]:
    result = int64.none
  else:
    result = res.integer.option

proc execute*(req: RedisRequestT[Option[uint64]]): Future[Option[uint64]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = uint64.none
  else:
    result = res.integer.uint64.option

proc execute*(req: RedisRequestT[Option[int32]]): Future[Option[int32]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = int32.none
  else:
    result = res.integer.int32.option

proc execute*(req: RedisRequestT[Option[uint32]]): Future[Option[uint32]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = uint32.none
  else:
    result = res.integer.uint32.option

proc execute*(req: RedisRequestT[Option[int16]]): Future[Option[int16]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = int16.none
  else:
    result = res.integer.int16.option

proc execute*(req: RedisRequestT[Option[uint16]]): Future[Option[uint16]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = uint16.none
  else:
    result = res.integer.uint16.option

proc execute*(req: RedisRequestT[RedisStrInt]): Future[int64] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.str.get("0").parseInt()

proc execute*(req: RedisRequestT[Option[RedisStrInt]]): Future[Option[int64]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = int64.none
  else:
    if res.str.isSome:
      result = res.str.get().parseInt().int64.option
    else:
      result = int64.none

proc execute*(req: RedisRequestT[float]): Future[float] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.double

proc execute*(req: RedisRequestT[RedisStrFloat]): Future[float] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.str.get("0").parseFloat()

proc execute*(req: RedisRequestT[Option[RedisStrFloat]]): Future[Option[float]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.kind == REDIS_MESSAGE_NIL:
    result = float.none
  else:
    if res.str.isSome:
      result = res.str.get().parseFloat().option
    else:
      result = float.none

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

proc execute*(req: RedisRequestT[SecureHash]): Future[SecureHash] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = parseSecureHash(res.str.get())

proc execute*[T](req: RedisRequestT[T]): Future[T] {.async.} =
  mixin fromRedisReq
  let res = await cast[RedisRequest](req).execute()
  result = T.fromRedisReq(res)

proc execute*(req: RedisArrayRequestT[bool]): Future[seq[bool]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
      result.add(x.boolean)

proc execute*(req: RedisArrayRequestT[Option[string]]): Future[seq[Option[string]]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
      case x.kind
      of REDIS_MESSAGE_NIL:
        result.add(string.none)
      of REDIS_MESSAGE_STRING, REDIS_MESSAGE_SIMPLESTRING:
        result.add(x.str)
      else:
        raise newException(RedisCommandError, "Wrong type of reply for string array")

proc execute*(req: RedisArrayRequestT[string]): Future[seq[string]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
      if x.str.isSome:
        result.add(x.str.get())

proc execute*(req: RedisArrayRequestT[int64]): Future[seq[int64]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        result.add(x.integer)

proc execute*(req: RedisArrayRequestT[RedisStrInt]): Future[seq[int64]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        result.add(x.str.get("0").parseInt())

proc execute*(req: RedisArrayRequestT[Option[RedisStrInt]]): Future[seq[Option[int64]]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        if x.str.isSome:
          result.add(x.str.get.parseInt().int64.option)
        else:
          result.add(int64.none)

proc execute*(req: RedisArrayRequestT[float]): Future[seq[float]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        result.add(x.double)

proc execute*(req: RedisArrayRequestT[RedisStrFloat]): Future[seq[float]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        result.add(x.str.get("0").parseFloat())

proc execute*(req: RedisArrayRequestT[Option[RedisStrFloat]]): Future[seq[Option[float]]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        if x.str.isSome:
          result.add(x.str.get.parseFloat().option)
        else:
          result.add(float.none)

proc execute*(req: RedisArrayRequestT[RedisIntBool]): Future[seq[bool]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        result.add(x.integer > 0)

proc execute*(req: RedisArrayRequestT[RedisStrBool]): Future[seq[bool]] {.async.} =
  result = @[]
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    for x in res.arr:
        result.add(x.str.get("") == "OK")

proc execute*[T](req: RedisArrayRequestT[T]): Future[seq[T]] {.async.} =
  mixin fromRedisReq
  let res = await cast[RedisRequest](req).execute()
  if res.kind != REDIS_MESSAGE_NIL:
    result = fromRedisReq(seq[T], res)

proc execute*(req: RedisCursorRequest): Future[RedisMessage] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  req.updateRedisCursor(res.arr[0].integer)
  result = res.arr[1]

template await*[T: RedisRequest](req: T): auto {.used.} =
  let f = execute(req)
  var internalTmpFuture: FutureBase = f
  yield internalTmpFuture
  (cast[typeof(f)](internalTmpFuture)).read()

proc cmd*(redis: Redis, cmd: string, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] =
  var req = newRedisRequest[RedisRequest](redis)
  req.addCmd(cmd, args)
  result = req.execute()

proc next*(cursor: RedisCursorRequest): Future[tuple[stop: bool, res: RedisMessage]] {.async.} =
  if cursor.reply.arr.len == 0 and not cursor.exhausted:
    cursor.reply = await cursor.execute()
  if cursor.reply.arr.len == 0:
    cursor.exhausted = true
    result = (true, nil)
  else:
    result = (false, cursor.reply.arr.pop())

proc next*(cursor: RedisCursorRequestT[string]): Future[tuple[stop: bool, res: string]] {.async.} =
  let res = await cast[RedisCursorRequest](cursor).next()
  if res[0]:
    result = (true, "")
  else:
    result = (false, res[1].str.get())


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
