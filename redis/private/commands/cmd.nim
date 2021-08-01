import std/[asyncdispatch, strutils]
import ../connection
import ../proto

export connection, proto

type
  RedisRequest* = ref RedisRequestObj
  RedisRequestObj* = object of RootObj
    redis*: Redis
    req*: seq[string]

proc newRedisRequest*(redis: Redis): RedisRequest =
  result.new
  result.redis = redis
  result.req = @[]

proc encodeCommand(cmd: string, args: varargs[RedisMessage, encodeRedis]): RedisMessage

proc add*(req: RedisRequest, data: RedisMessage): RedisRequest {.discardable.} =
  result = req
  result.req = result.req & data.prepareRequest()

proc add*(req: RedisRequest, data: varargs[RedisMessage, encodeRedis]): RedisRequest {.discardable.} =
  result = req
  for x in data:
    req.add(x)

proc addCmd*(req: RedisRequest, cmd: string, args: varargs[RedisMessage, encodeRedis]): RedisRequest {.discardable.} =
  result = req.add(encodeCommand(cmd, args))

proc execute*(req: RedisRequest): Future[RedisMessage] {.async.} =
  await req.redis.sendLine(req.req)
  result = await req.redis.parseResponse()

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