import std/[tables, strutils, times, asyncdispatch]
import ./exceptions
import ./buffered_socket

const
  REDIS_READER_MAX_BUF: int = 1024*16
  DEFAULT_POOLSIZE = 5

type
  Redis* = ref RedisObj
  RedisObj* = object of RootObj
    pool: seq[RedisConn]
    current: int
    host: string
    port: asyncdispatch.Port
    db: int
    timeout: int
    username: ref string
    password: ref string
    needAuth: bool
    closing: bool
  
  RedisConn* = ref RedisConnObj
  RedisConnObj* = object of RootObj
    inuse: bool
    authenticated: bool
    connected: bool
    sock: AsyncBufferedSocket

proc newRedisConn(): RedisConn
proc disconnect[T: RedisConn | RedisConnObj](redis: var T)

proc connect*(pool: Redis) {.async.} =
  for i in 0 ..< pool.pool.len:
    template conn: untyped = pool.pool[i]
    if conn.isNil:
      conn = newRedisConn()
    if not conn.connected:
      let connFut = conn.sock.connect(pool.host, pool.port)
      var success = false
      if pool.timeout > 0:
        success = await connFut.withTimeout(pool.timeout)
      else:
        await connFut
        success = true
      if success:
        conn.connected = true
      else:
        conn.connected = false
        raise newException(RedisConnectionError, "Timeout connecting to redis")

proc acquire*(pool: Redis): Future[RedisConn] {.async.} =
  ## Retrieves next non-in-use async socket for request
  if pool.closing:
    raise newException(RedisConnectionError, "Connection is safely closing now")
  let stime = getTime()
  while true:
    template s: untyped = pool.pool[pool.current]
    if not s.inuse:
      s.inuse = true
      return s
    pool.current.inc()
    pool.current = pool.current.mod(pool.pool.len)
    if pool.timeout > 0:
      let diff = (getTime() - stime).inMilliseconds()
      if diff > pool.timeout:
        raise newException(RedisConnectionError, "Failed to acquire connection")
    if pool.current == 0:
      await sleepAsync(300)

proc release*(redis: RedisConn) =
  redis.inuse = false

proc newRedis*(host: string, port: int, db: int, username: ref string = nil, password: ref string = nil, poolsize: int = DEFAULT_POOLSIZE, timeout: int = 0): Redis =
  result.new
  result.pool.setLen(poolsize)
  result.current = 0
  result.host = host
  result.port = asyncdispatch.Port(port)
  result.db = db
  result.timeout = timeout
  result.username = username
  result.password = password
  result.needAuth = not result.username.isNil
  result.closing = false

proc close*(pool: Redis) {.async.}=
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

template withRedis*(t: Redis, x: untyped) = 
  var redis {.inject.} = await t.acquire()
  x
  redis.release()

proc readLine*(redis: RedisConn): Future[string] {.async.} =
  result = await redis.sock.recvLine(maxLen = REDIS_READER_MAX_BUF)

proc readRawString*(redis: RedisConn, length: int): Future[string] {.async.} =
  result = await redis.sock.recv(length)
  if result.len < length:
    echo "Raw string read failed"

proc sendLine*(redis: RedisConn, data: seq[string]) {.async.} =
  var line = data.join("\r\L")
  line = line & "\r\L"
  #echo "CMD: ", line
  let size {.used.} = await redis.sock.send(line)

proc needsAuth*(redis: RedisConn): bool = redis.needsAuth and not redis.authenticated

#------- pvt

proc disconnect[T: RedisConn | RedisConnObj](redis: var T) =
  if redis.connected:
    redis.sock.close()
    #TODO Add calling all waiting callbacks
  redis.connected = false
  redis.inuse = false

proc `=destroy`(redis: var RedisConnObj) =
  if redis.connected:
    redis.disconnect()

proc newRedisConn(): RedisConn =
  result.new()
  result.inuse = false
  result.authenticated = false
  result.connected = false
  result.sock = newAsyncBufferedSocket(bufSize = REDIS_READER_MAX_BUF)
  #result.queue = newTable[ int32, tuple[cur: Cursor[AsyncMongo], fut: Future[seq[Bson]]] ]()
