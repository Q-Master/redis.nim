import std/[asyncdispatch, strutils, tables, times, options]
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
    CLIENT_TYPE_ALL = ""
    CLIENT_TYPE_NORMAL = "normal"
    CLIENT_TYPE_MASTER = "master"
    CLIENT_TYPE_REPLICA = "replica"
    CLIENT_TYPE_PUBSUB = "pubsub"
  
  HostPort = object
    host*: string
    port*: Port

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
  
  RedisClientInfoRequest* = ref object of RedisRequest

# AUTH [username] password 
proc auth*(redis: Redis, password: string, login: Option[string] = string.none): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  if login.isSome:
    result.addCmd("AUTH", login.get(), password)
  else:
    result.addCmd("AUTH", password)

# CLIENT CACHING YES|NO 
proc clientCaching*(redis: Redis, enabled: bool): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("CLIENT CACHING", (if enabled: "YES" else: "NO"))

# CLIENT GETNAME 
proc clientGetName*(redis: Redis): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("CLIENT GETNAME")

# CLIENT GETREDIR 
proc clientGetRedir*(redis: Redis): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("CLIENT GETREDIR")

# CLIENT ID 
proc clientID*(redis: Redis): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("CLIENT ID")

# CLIENT INFO 
proc parseClientInfo(line: string): ClientInfo

proc fromRedisReq*(_: type[ClientInfo], req: RedisMessage): ClientInfo =
  var str = req.str.get("")
  str.stripLineEnd()
  result = str.parseClientInfo()

proc clientInfo*(redis: Redis): RedisRequestT[ClientInfo] =
  result = newRedisRequest[RedisRequestT[ClientInfo]](redis)
  result.addCmd("CLIENT INFO")

# CLIENT LIST [TYPE normal|master|replica|pubsub] [ID client-id [client-id ...]] 
proc fromRedisReq*(_: type[seq[ClientInfo]], req: RedisMessage): seq[ClientInfo] =
  result = @[]
  var str = req.str.get("")
  str.stripLineEnd()
  for s in str.split('\n'):
    result.add(s.parseClientInfo())

proc clientList*(redis: Redis, clientType: ClientType = CLIENT_TYPE_ALL, clientIDs: seq[int64] = @[]): RedisArrayRequest[ClientInfo] =
  result = newRedisRequest[RedisArrayRequest[ClientInfo]](redis)
  result.addCmd("CLIENT LIST")
  if clientType != CLIENT_TYPE_ALL:
    result.add("TYPE", $clientType)
  if clientIDs.len > 0:
    result.add("ID")
    for id in clientIDs:
      result.add(id)

# CLIENT SETNAME connection-name 
proc clientSetName*(redis: Redis, name: string): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("CLIENT SETNAME", name)

# ECHO message 
proc echo*(redis: Redis, msg: string): RedisRequestT[string] =
  result = newRedisRequest[RedisRequestT[string]](redis)
  result.addCmd("ECHO", msg)

# HELLO [protover [AUTH username password] [SETNAME clientname]]
proc fromRedisReq*(_: type[RedisHello], req: RedisMessage): RedisHello =
  result.new
  case req.kind
  of REDIS_MESSAGE_ARRAY:
    # RESP2 reply
    result.server = req.arr[1].str.get()
    result.version = req.arr[3].str.get()
    result.proto = req.arr[5].integer.int
    result.id = req.arr[7].integer.int
    result.mode = req.arr[9].str.get()
    result.role = req.arr[11].str.get()
    for m in req.arr[13].arr:
      result.modules.add(m.str.get())
  of REDIS_MESSAGE_MAP:
    # RESP3 reply
    result.server = req.map["server"].str.get()
    result.version = req.map["version"].str.get()
    result.proto = req.map["proto"].integer.int
    result.id = req.map["id"].integer.int
    result.mode = req.map["mode"].str.get()
    result.role = req.map["role"].str.get()
    for m in req.map["modules"].arr:
      result.modules.add(m.str.get())
  else:
    raise newException(RedisCommandError, "Unexpected reply for HELLO")

proc hello*(redis: Redis, 
  protoVer: Option[int] = int.none, 
  login: Option[string] = string.none, 
  password: Option[string] = string.none, 
  clientName: Option[string] = string.none): RedisRequestT[RedisHello] =
  result = newRedisRequest[RedisRequestT[RedisHello]](redis)
  result.addCmd("HELLO")
  if protoVer.isSome:
    result.add(protoVer.get())
  if password.isSome:
    result.add("AUTH")
    if login.isSome:
      result.add(login.get())
    result.add(password.get())
  if clientName.isSome:
    result.add("SETNAME", clientName.get())

# PING [message] 
proc ping*(redis: Redis, msg: Option[string] = string.none): RedisRequestT[string] =
  result = newRedisRequest[RedisRequestT[string]](redis)
  result.addCmd("PING")
  if msg.isSome:
    result.add(msg.get())
#[ 
  var res: RedisMessage
  if msg.len > 0:
    res = await redis.cmd("PING", msg) 
  else: 
    res = await redis.cmd("PING")
  case res.kind
  of REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_STRING:
    if msg.len > 0:
      result = res.str.get()
    else:
      if res.str.get() == "PONG":
        result = ""
      else:
        raise newException(RedisCommandError, "Wrong answer to PING: " & res.str.get())
  of REDIS_MESSAGE_ARRAY:
    if res.arr[0].str.get() == "PONG":
      if msg.len > 0:
        result = res.arr[1].str.get()
      else:
        result = ""
    else:
      raise newException(RedisCommandError, "Wrong answer to PING: " & res.arr[0].str.get())
  else:
    raise newException(RedisCommandError, "Wrong answer to PING: " & (if res.kind == REDIS_MESSAGE_STRING: res.str.get() else: "Unknown (type: " & $res.kind & ")"))
]#

# QUIT
proc quit*(redis: Redis): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("QUIT")

# RESET
proc reset*(redis: Redis): RedisRequestT[string] =
  result = newRedisRequest[RedisRequestT[string]](redis)
  result.addCmd("RESET")

# SELECT index 
proc select*(redis: Redis, db: int = 0): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("SELECT", db)

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