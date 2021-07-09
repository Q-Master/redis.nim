import std/[asyncdispatch, strutils, tables, times]
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

type
  RedisHello* = ref RedisHelloObj
  RedisHelloObj* = object of RootObj
    server*: string
    version*: string
    proto*: int
    id*: int
    mode*: string
    role*: string
    modules*: seq[string]
  
  ClientType* = enum
    CLIENT_TYPE_ALL = -1
    CLIENT_TYPE_NORMAL = 0
    CLIENT_TYPE_MASTER = 1
    CLIENT_TYPE_REPLICA = 2
    CLIENT_TYPE_PUBSUB = 3
  
  HostPort = object
    host: string
    port: Port

  ClientInfo* = ref ClientInfoObj
  ClientInfoObj* = object of RootObj
    id*: uint64
    name*: string
    caddr*: HostPort
    laddr*: HostPort
    fd*: int
    age*: Duration
    idle*: Duration
    #flags*:
    db*: int
    sub*: int
    psub*: int
    multi*: int
    qbuf*: uint
    qbufFree*: uint64
    obl*: uint64
    oll*: uint
    omem*: uint64
    #events*:
    cmd*: string
    argvMem*: uint64
    totMem*: uint64
    redir*: int64
    user*: string
    
proc auth*(redis: Redis, password: string, login: string = "") {.async.} =
  let res = (if login.len == 0: await redis.cmd("AUTH", password) else: await redis.cmd("AUTH", login, password))
  if res.kind == REDIS_MESSAGE_ERROR:
    raise newException(RedisAuthError, res.error)

proc clientCaching*(redis: Redis, enabled: bool) {.async.} =
  let res = await redis.cmd("CLIENT CACHING", (if enabled: "YES" else: "NO"))
  if res.str[] != "OK":
    raise newException(RedisCommandError, "Wrong answer to CLIENT CACHING")

proc clientGetRedir*(redis: Redis): Future[int64] {.async.} =
  let res = await redis.cmd("CLIENT GETREDIR")
  result = res.integer

const typeToStringType = {
  CLIENT_TYPE_MASTER: "master",
  CLIENT_TYPE_NORMAL: "normal",
  CLIENT_TYPE_PUBSUB: "pubsub",
  CLIENT_TYPE_REPLICA: "replica"}.toTable()
proc parseClientInfo(line: string): ClientInfo
proc clientInfo*(redis: Redis): Future[ClientInfo] {.async.} =
  let res = await redis.cmd("CLIENT INFO")
  res.str[].stripLineEnd()
  result = res.str[].parseClientInfo()

proc clientList*(redis: Redis, clientType: ClientType = CLIENT_TYPE_ALL, clientIDs: seq[int64] = @[]): Future[seq[ClientInfo]] {.async.} =
  result = @[]
  var args: seq[RedisMessage] = @[]
  if clientType != CLIENT_TYPE_ALL:
    args.add("TYPE".encodeRedis())
    args.add(typeToStringType[clientType].encodeRedis())
  if clientIDs.len > 0:
    args.add("ID".encodeRedis())
    for id in clientIDs:
      args.add(id.encodeRedis())
  let res = await redis.cmd("CLIENT LIST", args = args)
  res.str[].stripLineEnd()
  for str in res.str[].split('\n'):
    result.add(str.parseClientInfo())

proc clientID*(redis: Redis): Future[uint64] {.async.} =
  let res = await redis.cmd("CLIENT ID")
  result = res.integer.uint64

proc clientKill*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  raise newException(RedisCommandNotYetImplementedError, "CLIENT KILL is not implemented yet")

proc clientReply*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  raise newException(RedisCommandNotYetImplementedError, "CLIENT REPLY is not implemented yet")

proc clientSetName*(redis: Redis, name: string) {.async.} =
  let res = await redis.cmd("CLIENT SETNAME", name)
  if res.str[] != "OK":
    raise newException(RedisCommandError, "Wrong answer to CLIENT SETNAME")

proc clientGetName*(redis: Redis): Future[ref string] {.async.} =
  let res = await redis.cmd("CLIENT GETNAME")
  if res.kind == REDIS_MESSAGE_NIL:
    result = nil
  else:
    result = res.str

proc clientTracking*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  raise newException(RedisCommandNotYetImplementedError, "CLIENT TRACKING is not implemented yet")

proc clientTrackingInfo*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  raise newException(RedisCommandNotYetImplementedError, "CLIENT TRACKING INFO is not implemented yet")

proc clientUnblock*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  raise newException(RedisCommandNotYetImplementedError, "CLIENT UNBLOCK is not implemented yet")

proc clientPause*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  raise newException(RedisCommandNotYetImplementedError, "CLIENT PAUSE is not implemented yet")

proc clientUnpause*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisMessage] {.async.} =
  raise newException(RedisCommandNotYetImplementedError, "CLIENT UNPAUSE is not implemented yet")

proc echo*(redis: Redis, msg: string): Future[string] {.async.} =
  let res = await redis.cmd("ECHO", msg)
  result = res.str[]

proc hello*(redis: Redis, args: varargs[RedisMessage, encodeRedis]): Future[RedisHello] {.async.} =
  let res = await redis.cmd("HELLO", args)
  result.new()
  case res.kind
  of REDIS_MESSAGE_ARRAY:
    # RESP2 reply
    result.server = res.arr[1].str[]
    result.version = res.arr[3].str[]
    result.proto = res.arr[5].integer.int
    result.id = res.arr[7].integer.int
    result.mode =  res.arr[9].str[]
    result.role =  res.arr[11].str[]
    for m in res.arr[13].arr:
      result.modules.add(m.str[])
  of REDIS_MESSAGE_MAP:
    # RESP3 reply
    result.server = res.map["server"].str[]
    result.version = res.map["version"].str[]
    result.proto = res.map["proto"].integer.int
    result.id = res.map["id"].integer.int
    result.mode =  res.map["mode"].str[]
    result.role =  res.map["role"].str[]
    for m in res.map["modules"].arr:
      result.modules.add(m.str[])
  else:
    raise newException(RedisCommandError, "Unexpected reply for HELLO")

proc ping*(redis: Redis, msg: string = ""): Future[string] {.async.} =
  var res: RedisMessage
  if msg.len > 0:
    res = await redis.cmd("PING", msg) 
  else: 
    res = await redis.cmd("PING")
  case res.kind
  of REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_STRING:
    if msg.len > 0:
      result = res.str[]
    else:
      if res.str[] == "PONG":
        result = ""
      else:
        raise newException(RedisCommandError, "Wrong answer to PING: " & res.str[])
  of REDIS_MESSAGE_ARRAY:
    if res.arr[0].str[] == "PONG":
      if msg.len > 0:
        result = res.arr[1].str[]
      else:
        result = ""
    else:
      raise newException(RedisCommandError, "Wrong answer to PING: " & res.arr[0].str[])
  else:
    raise newException(RedisCommandError, "Wrong answer to PING: " & (if res.kind == REDIS_MESSAGE_STRING: res.str[] else: "Unknown"))

proc quit*(redis: Redis) {.async.} =
  let res = await redis.cmd("QUIT")
  if res.str[] != "OK":
    raise newException(RedisCommandError, "Unexpected reply for QUIT")

proc reset*(redis: Redis): Future[RedisMessage] {.async.} =
  let res = await redis.cmd("RESET")
  if res.str[] != "RESET":
    raise newException(RedisCommandError, "Unexpected reply for RESET")
  if redis.needsAuth:
    raise newException(RedisAuthError, "Connection must be reauthenticated")

proc select*(redis: Redis, db: int = 0): Future[RedisMessage] {.async.} =
  let res = await redis.cmd("SELECT", db)
  if res.str[] != "OK":
    raise newException(RedisCommandError, "Unexpected reply for SELECT")

#------- pvt
template tohp(hp: string): HostPort =
  let aport = kv[1].split(':')
  HostPort(host: aport[0], port: Port(aport[1].parseInt()))

proc parseClientInfo(line: string): ClientInfo =
  result.new()
  let params = line.split(' ')
  for param in params:
    let kv = param.split('=', 1)
    case kv[0]
    of "id":
      result.id = kv[1].parseInt().uint64
    of "name":
      result.name = kv[1]
    of "addr":
      result.caddr = tohp(kv[1])
    of "laddr":
      result.laddr = tohp(kv[1])
    of "fd":
      result.fd = kv[1].parseInt()
    of "age":
      result.age = initDuration(seconds=kv[1].parseInt())
    of "idle":
      result.idle = initDuration(seconds=kv[1].parseInt())
    of "db":
      result.db = kv[1].parseInt()
    of "sub":
      result.sub = kv[1].parseInt()
    of "psub":
      result.psub = kv[1].parseInt()
    of "multi":
      result.multi = kv[1].parseInt()
    of "qbuf":
      result.qbuf = kv[1].parseInt().uint
    of "qbuf-free":
      result.qbufFree = kv[1].parseInt().uint64
    of "obl":
      result.obl = kv[1].parseInt().uint64
    of "oll":
      result.oll = kv[1].parseInt().uint
    of "omem":
      result.omem = kv[1].parseInt().uint64
    of "cmd":
      result.cmd = kv[1]
    of "argv-mem":
      result.argvMem = kv[1].parseInt().uint64
    of "tot-mem":
      result.totMem = kv[1].parseInt().uint64
    of "redir":
      result.redir = kv[1].parseInt()
    of "user":
      result.user = kv[1]
    else:
      discard