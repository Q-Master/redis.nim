import std/asyncdispatch
import ../connection
import ../proto

export connection, proto

proc cmd*(redis: Redis, cmd: string, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] =
  let data = encodeCommand(cmd, args).prepareRequest()
  proc sendRcvCmd(): Future[RedisMessage] {.async.} =
    await redis.sendLine(data)
    result = await redis.parseResponse()
  sendRcvCmd()

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
