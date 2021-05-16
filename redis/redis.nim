import std/asyncdispatch
import private/[connection, exceptions, proto]

export connection except readLine, readRawString
export Redis, RedisObj, RedisMessage, RedisMessageObj, RedisMessageTypes
export exceptions

proc sendCommand*(redis: Redis, cmd: string, args: varargs[typed]): Future[RedisMessage] {.async.} =
  let data = encodeCommand(cmd, args)
  await sendLine(redis, data)
  result = await redis.parseResponse()

proc sendCommand*(pool: RedisPool, cmd: string, args: varargs[typed]): Future[RedisMessage] {.async.} =
  withRedis pool:
    result = await redis.sendCommand(cmd, args)
