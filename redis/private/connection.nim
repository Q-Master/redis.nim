import std/[tables, strutils, times, asyncdispatch]
import ./exceptions
import ./buffered_socket

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
    closing: bool
  
  Redis* = ref RedisObj
  RedisObj* = object of RootObj
    inuse: bool
    authenticated: bool
    connected: bool
    sock: AsyncBufferedSocket
    #queue:         TableRef[int32, tuple[cur: Cursor[AsyncMongo], fut: Future[seq[Bson]]] ]

proc newRedis(): Redis
proc disconnect[T: Redis | RedisObj](redis: var T)

proc acquire*(pool: RedisPool, timeout: int = 0): Future[Redis] {.async.} =
  ## Retrieves next non-in-use async socket for request
  if pool.closing:
    raise newException(RedisConnectionError, "Connection is safely closing now")
  let stime = getTime()
  while true:
    template s: untyped = pool.pool[pool.current]
    if s.isNil:
      s = newRedis()
    try:
      if not s.inuse:
        s.inuse = true
        if not s.connected:
          let connFut = s.sock.connect(pool.host, pool.port)
          var success = false
          if timeout > 0:
            success = await connFut.withTimeout(timeout)
          else:
            await connFut
            success = true
          if success:
            s.connected = true
            #if pool.needAuth and not s.authenticated:
              #s.authenticated = await pool[pool.authDb()].authenticateScramSha1(pool.username, pool.password, s)
          else:
            s.inuse = false
            raise newException(RedisConnectionError, "Timeout connecting to redis")
        return s
    except OSError:
      s.inuse = false
    pool.current.inc()
    pool.current = pool.current.mod(pool.pool.len)
    if timeout > 0:
      let diff = (getTime() - stime).inMilliseconds()
      if diff > timeout:
        raise newException(RedisConnectionError, "Timeout connecting to redis")
    if pool.current == 0:
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
  result.closing = false

proc close*(pool: RedisPool) {.async.}=
  pool.closing = true
  var closed: bool = false
  while not closed:
    closed = true
    for i in 0 ..< pool.pool.len:
      template s: untyped = pool.pool[i]
      if not s.isNil:
        if s.inuse:
          closed = false
        else:
          s.disconnect()
    await sleepAsync(1)

template withRedis*(t: RedisPool, timeout: int = 0, x: untyped) = 
  var redis {.inject.} = await t.acquire(timeout)
  x
  redis.release()

proc readLine*(redis: Redis): Future[string] {.async.} =
  result = await redis.sock.recvLine(maxLen = REDIS_READER_MAX_BUF)

proc readRawString*(redis: Redis, length: int): Future[string] {.async.} =
  result = await redis.sock.recv(length)
  if result.len < length:
    echo "Raw string read failed"

proc sendLine*(redis: Redis, data: seq[string]) {.async.} =
  var line = data.join("\r\L")
  line = line & "\r\L"
  #echo "CMD: ", line
  let size {.used.} = await redis.sock.send(line)

proc needsAuth*(redis: Redis): bool = redis.needsAuth

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
  result.new()
  result.inuse = false
  result.authenticated = false
  result.connected = false
  result.sock = newAsyncBufferedSocket(bufSize = REDIS_READER_MAX_BUF)
  #result.queue = newTable[ int32, tuple[cur: Cursor[AsyncMongo], fut: Future[seq[Bson]]] ]()
