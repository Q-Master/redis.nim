import std/asyncdispatch
import ../connection
import ../proto

export connection, proto

template cmd*(redis: Redis, cmd: string, args: varargs[RedisMessage, encodeRedis]): untyped =
  block:
    proc realCmd(): Future[RedisMessage] {.async.} =
      let data = encodeCommand(cmd, args).prepareRequest()
      await sendLine(redis, data)
      result = await redis.parseResponse()
    realCmd()

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
