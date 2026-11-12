import std/[tables, strutils, times, monotimes, asyncdispatch, net]
import pkg/networkutils/buffered_socket
import ./exceptions


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


proc newRedisConn(timeout: int): RedisConn
proc disconnect[T: RedisConn | RedisConnObj](redis: var T): Future[void]
proc connectConn(pool: Redis, conn: RedisConn): Future[void]   # определение ниже, {.async.}


proc connect*(pool: Redis) {.async.} =
  ## Optional connection heating for pool
  ## `acquire` now connects if not connected lazily
  for i in 0 ..< pool.pool.len:
    if pool.pool[i].isNil:
      pool.pool[i] = newRedisConn(pool.timeout)
    if not pool.pool[i].connected:
      await pool.connectConn(pool.pool[i])


proc acquire*(pool: Redis): Future[RedisConn] {.async.} =
  ## Acquires first free connection from pool
  ## Выдаёт свободное соединение, при необходимости (пере)подключая его.
  if pool.closing:
    raise newException(RedisConnectionError, "Connection is safely closing now")
  let stime = getMonoTime()
  var failures = 0
  var s: RedisConn
  while true:
    s = pool.pool[pool.current]
    if s.isNil:
      s = newRedisConn(pool.timeout)
      pool.pool[pool.current] = s
    if not s.inuse:
      s.inuse = true
      if s.connected:
        return s
      try:
        await pool.connectConn(s)
        return s
      except CatchableError as e:
        s.inuse = false
        inc failures
        if failures >= pool.pool.len:
          raise newException(RedisConnectionError, "Failed to (re)connect to redis: " & e.msg, e)
    pool.current = (pool.current + 1) mod pool.pool.len
    if pool.timeout > 0 and (getMonoTime() - stime).inMilliseconds > pool.timeout:
      raise newException(RedisConnectionError, "Failed to acquire connection")
    if pool.current == 0:
      await sleepAsync(300)


proc release*(redis: RedisConn) =
  redis.inuse = false


proc newRedis*(host: string, port: int, db: int, username: ref string = nil, password: ref string = nil, poolsize: int = DEFAULT_POOLSIZE, timeout: int = 0): Redis =
  doAssert poolsize > 0, "poolsize must be positive"
  result.new
  result.pool.setLen(poolsize)
  result.current = 0
  result.host = host
  result.port = asyncdispatch.Port(port)
  result.db = db
  result.timeout = timeout
  result.username = username
  result.password = password
  result.needAuth = not result.password.isNil
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
          await s.disconnect()
    await sleepAsync(1)


template withRedis*(t: Redis, x: untyped) = 
  var redis {.inject.} = await t.acquire()
  try:
    x
  finally:
    redis.release()


template transportGuard(redis: RedisConn, body: untyped) =
  try:
    body
  except ConnectionClosedError, TimeoutError, OSError, LineTooLongError:
    redis.connected = false
    raise newException(RedisConnectionError, getCurrentExceptionMsg(), getCurrentException())


proc readLine*(redis: RedisConn): Future[string] {.async.} =
  transportGuard(redis):
    result = await redis.sock.recvLine(maxLen = REDIS_READER_MAX_BUF)


proc readRawString*(redis: RedisConn, length: int): Future[string] {.async.} =
  transportGuard(redis):
    result = await redis.sock.recv(length)


proc sendLine*(redis: RedisConn, data: seq[string]) {.async.} =
  var line = data.join("\r\L")
  line = line & "\r\L"
  #echo "CMD: ", line
  transportGuard(redis):
    let size {.used.} = await redis.sock.send(line)


#------- pvt


proc disconnect[T: RedisConn | RedisConnObj](redis: var T): Future[void] =
  if redis.connected:
    result = redis.sock.close()
    #TODO Add calling all waiting callbacks
  redis.connected = false
  redis.inuse = false


proc newSocket(timeout: int): AsyncBufferedSocket =
  newAsyncBufferedSocket(inBufSize = REDIS_READER_MAX_BUF, timeout = (if timeout > 0: timeout else: -1))


proc respCommand(args: openArray[string]): string =
  # need here to not import cmds
  result = "*" & $args.len & "\r\L"
  for a in args:
    result.add("$" & $a.len & "\r\L")
    result.add(a)
    result.add("\r\L")


proc handshake(pool: Redis, conn: RedisConn) {.async.} =
  # need here to not import cmds
  # RESP2-handshake: AUTH + SELECT
  if pool.needAuth:
    var args = @["AUTH"]
    if not pool.username.isNil:
      args.add(pool.username[])
    args.add(pool.password[])
    transportGuard(conn):
      discard await conn.sock.send(respCommand(args))
    let resp = await conn.readLine()
    if resp.len == 0 or resp[0] != '+':
      raise newException(RedisAuthError, "AUTH failed: " & resp)
    conn.authenticated = true
  if pool.db != 0:
    transportGuard(conn):
      discard await conn.sock.send(respCommand(["SELECT", $pool.db]))
    let resp = await conn.readLine()
    if resp.len == 0 or resp[0] != '+':
      raise newException(RedisConnectionError, "SELECT failed: " & resp)


proc connectConn(pool: Redis, conn: RedisConn) {.async.} =
  # Reinitializing socket, because old one is unusable after failure
  if not conn.sock.isNil:
    try:
      await conn.sock.close()
    except CatchableError:
      discard
  conn.sock = newSocket(pool.timeout)
  conn.connected = false
  conn.authenticated = false
  try:
    await conn.sock.connect(pool.host, pool.port)
  except TimeoutError, OSError:
    raise newException(RedisConnectionError, "Connect to redis failed: " & getCurrentExceptionMsg(), getCurrentException())
  await pool.handshake(conn)
  conn.connected = true


proc newRedisConn(timeout: int): RedisConn =
  result.new()
  result.inuse = false
  result.authenticated = false
  result.connected = false
  result.sock = newSocket(timeout)
