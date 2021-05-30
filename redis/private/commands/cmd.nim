import std/asyncdispatch
import ../connection
import ../proto

proc cmd*(redis: Redis, cmd: string, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  let data = encodeCommand(cmd, args).prepareRequest()
  await sendLine(redis, data)
  result = await redis.parseResponse()
