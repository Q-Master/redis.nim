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
