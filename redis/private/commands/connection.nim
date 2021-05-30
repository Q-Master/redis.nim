import std/[asyncdispatch, strutils]
import ./cmd
import ../connection
import ../proto
import ../exceptions

#[
  Block of connection commands
    AUTH
    CLIENT CACHING
    CLIENT GETNAME
    CLIENT GETREDIR
    CLIENT ID
    CLIENT INFO
    CLIENT KILL
    CLIENT LIST
    CLIENT PAUSE
    CLIENT REPLY
    CLIENT SETNAME
    CLIENT TRACKING
    CLIENT TRACKINGINFO
    CLIENT UNBLOCK
    CLIENT UNPAUSE
    ECHO
    HELLO
    PING
    QUIT
    RESET
    SELECT
]#

proc auth*(redis: Redis): Future[RedisMessage] {.async.} =
  discard

proc clientCaching(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientGetName(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientGetRedir(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientID(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientInfo(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientKill(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientList(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientPause(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientReply(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientSetName(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientTracking(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientTrackingInfo(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientUnblock(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc clientUnpause(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}
proc client*(redis: Redis, kind: string, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.}=
  case kind.toUpper()
  of "CACHING":
    result = await redis.clientCaching(args)
  of "GETNAME":
    result = await redis.clientGetName(args)
  of "GETREDIR":
    result = await redis.clientGetRedir(args)
  of "ID":
    result = await redis.clientID(args)
  of "INFO":
    result = await redis.clientInfo(args)
  of "KILL":
    result = await redis.clientKill(args)
  of "LIST":
    result = await redis.clientList(args)
  of "PAUSE":
    result = await redis.clientPause(args)
  of "REPLY":
    result = await redis.clientReply(args)
  of "SETNAME":
    result = await redis.clientSetName(args)
  of "TRACKING":
    result = await redis.clientTracking(args)
  of "TRACKINGINFO":
    result = await redis.clientTrackingInfo(args)
  of "UNBLOCK":
    result = await redis.clientUnblock(args)
  of "UNPAUSE":
    result = await redis.clientUnpause(args)
  else:
    raise newException(RedisCommandError, "CLIENT command has no " & kind & " subcommand")

proc echo*(redis: Redis): Future[RedisMessage] {.async.} =
  discard

proc hello*(redis: Redis): Future[RedisMessage] {.async.} =
  discard

proc ping*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[string] {.async.} =
  if args.len > 0:
    if args[0].kind != REDIS_MESSAGE_STRING or args.len > 1:
      raise newException(RedisCommandError, "PING can accept only 1 string parameter")
  let res = await redis.cmd("PING", args)
  case res.kind
  of REDIS_MESSAGE_STRING:
    if args.len > 0:
      result = res.str[]
    else:
      if res.str[] == "PONG":
        result = ""
      else:
        raise newException(RedisCommandError, "Wrong answer to PING: " & res.str[])
  of REDIS_MESSAGE_ARRAY:
    if res.arr[0].str[] == "PONG":
      if args.len > 0:
        result = res.arr[1].str[]
      else:
        result = ""
    else:
      raise newException(RedisCommandError, "Wrong answer to PING: " & res.arr[0].str[])
  else:
    raise newException(RedisCommandError, "Wrong answer to PING: " & (if res.kind == REDIS_MESSAGE_STRING: res.str[] else: "Unknown"))

proc quit*(redis: Redis): Future[RedisMessage] {.async.} =
  discard

proc reset*(redis: Redis): Future[RedisMessage] {.async.} =
  discard

proc select*(redis: Redis): Future[RedisMessage] {.async.} =
  discard

#------- pvt

proc clientCaching(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientGetName(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientGetRedir(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientID(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientInfo(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientKill(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientList(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientPause(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientReply(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientSetName(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientTracking(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientTrackingInfo(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientUnblock(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard
proc clientUnpause(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  discard