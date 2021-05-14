import std/asyncdispatch
import private/[connection, exceptions, proto]

export connection except readLine, readRawString
export exceptions

proc readResponse*(redis: Redis): Future[RedisMessage] {.async.} =
  result = await redis.parseResponse()

