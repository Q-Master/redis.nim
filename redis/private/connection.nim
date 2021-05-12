import std/tables
import std/[asyncnet, asyncdispatch]

const
  REDIS_READER_MAX_BUF: int = 1024*16
  DEFAULT_POOLSIZE = 5

type
  RedisPool* = ref RedisPoolObj
  RedisPoolObj* = object of RootObj
    pool: seq[Redis]
    current: int
    host: string
    port: asyncdispatch.Port
    db: int
    username: ref string
    password: ref string
    needAuth: bool
  
  Redis* = ref RedisObj
  RedisObj* = object of RootObj
    inuse: bool
    authenticated: bool
    connected: bool
    sock: AsyncSocket
    #queue:         TableRef[int32, tuple[cur: Cursor[AsyncMongo], fut: Future[seq[Bson]]] ]

proc newRedis(): Redis

proc acquire*(rconn: RedisPool): Future[Redis] {.async.} =
  ## Retrieves next non-in-use async socket for request
  while true:
    template s: untyped = rconn.pool[rconn.current]
    if s.isNil:
      s = newRedis()
    if not s.inuse:
      s.inuse = true
      if not s.connected:
        try:
          await s.sock.connect(rconn.host, rconn.port)
          s.connected = true
          #if rconn.needAuth and not s.authenticated:
            #s.authenticated = await rconn[rconn.authDb()].authenticateScramSha1(rconn.username, rconn.password, s)
        except OSError:
          continue
      return s
    rconn.current.inc()
    rconn.current = rconn.current.mod(rconn.pool.len)
    if rconn.current == 0:
      await sleepAsync(1)

proc release*(redis: Redis) =
  redis.inuse = false

proc newRedisPool*(host: string, port: int, db: int, username: ref string = nil, password: ref string = nil, poolsize: int = DEFAULT_POOLSIZE): RedisPool =
  result.new
  result.pool.setLen(poolsize)
  result.current = 0
  result.host = host
  result.port = asyncdispatch.Port(port)
  result.db = db
  result.username = username
  result.password = password
  result.needAuth = not result.username.isNil


proc readLine*(redis: Redis): Future[string] {.async.} =
  result = await redis.sock.recvLine(maxLength = REDIS_READER_MAX_BUF)

proc readRawString*(redis: Redis, length: int): Future[string] {.async.} =
  result.setLen(length)
  let realsize = await redis.sock.recvInto(result.addr, length)
  if realsize < length:
    echo "Raw string read failed"

#------- pvt

proc disconnect[T: Redis | RedisObj](redis: var T) =
  if redis.connected:
    redis.sock.close()
    #TODO Add calling all waiting callbacks
  redis.connected = false
  redis.inuse = false

proc `=destroy`(redis: var RedisObj) =
  if redis.connected:
    redis.disconnect()

proc newRedis(): Redis =
  ## Constructor for "locked" socket
  result.new()
  result.inuse = false
  result.authenticated = false
  result.connected = false
  result.sock = newAsyncSocket()
  #result.queue = newTable[ int32, tuple[cur: Cursor[AsyncMongo], fut: Future[seq[Bson]]] ]()
