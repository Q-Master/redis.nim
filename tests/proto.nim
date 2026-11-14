import unittest
import std/[asyncdispatch, asyncnet, strutils, options, tables, sets]
import asyncredis/commands/cmd
import asyncredis/connection
import asyncredis/proto
import asyncredis/exceptions


suite "Protocol":
  setup:
    discard

  test "Encoding: command args are flat bulk strings":
    var req = RedisRequest.new
    req.initRedisRequest(nil)
    req.addCmd("PONG")
    check req.req.kind == REDIS_MESSAGE_ARRAY
    check req.req.arr[0].str == "PONG".option

    req.addCmd("TEST", 1, 1.0, "string", true)
    check req.req.arr.len == 5
    check req.req.arr[0].str.get() == "TEST"
    for i in 1 .. 4:
      check req.req.arr[i].kind == REDIS_MESSAGE_STRING
    check req.req.arr[1].str.get() == "1"
    check req.req.arr[2].str.get() == "1.0"
    check req.req.arr[3].str.get() == "string"
    check req.req.arr[4].str.get() == "1"

    req.extend([1, 2, 3, 4, 5])
    check req.req.arr.len == 10
    check req.req.arr[5].str.get() == "1"
    check req.req.arr[9].str.get() == "5"

    let parts = req.req.prepareRequest()
    check parts[0] == "*10"
    for i in countup(1, parts.len - 1, 2):
      check parts[i][0] == '$'

  test "Encoding: encodeRedis preserves RESP types":
    let msg = RedisMessage(kind: REDIS_MESSAGE_ARRAY, arr: @[
      encodeRedis(1), encodeRedis(1.0), encodeRedis("string"),
      encodeRedis(true), encodeRedis([1, 2, 3, 4, 5])])
    check msg.arr[0].kind == REDIS_MESSAGE_INTEGER and msg.arr[0].integer == 1
    check msg.arr[1].kind == REDIS_MESSAGE_DOUBLE and msg.arr[1].double == 1.0
    check msg.arr[2].kind == REDIS_MESSAGE_STRING and msg.arr[2].str.get() == "string"
    check msg.arr[3].kind == REDIS_MESSAGE_BOOL and msg.arr[3].boolean
    check msg.arr[4].kind == REDIS_MESSAGE_ARRAY
    check msg.arr[4].arr.len == 5
    check msg.arr[4].arr[0].integer == 1
    check msg.arr[4].arr[4].integer == 5

  test "Encoding: command array args are flattened":
    var req = RedisRequest.new
    req.initRedisRequest(nil)
    req.addCmd("MGET", ["k1", "k2", "k3"])
    check req.req.arr.len == 4
    check req.req.arr[0].str.get() == "MGET"
    check req.req.arr[3].str.get() == "k3"
    for e in req.req.arr:
      check e.kind == REDIS_MESSAGE_STRING

  test "Encoding: command mixed scalar and array args":
    var req = RedisRequest.new
    req.initRedisRequest(nil)
    req.addCmd("TEST", 1, 1.0, "string", true, [1,2,3,4,5])
    check req.req.arr.len == 10
    check req.req.arr[1].str.get() == "1"
    check req.req.arr[4].str.get() == "1"
    check req.req.arr[9].str.get() == "5"
    let parts = req.req.prepareRequest()
    check parts[0] == "*10"
    for i in countup(1, parts.len - 1, 2):
      check parts[i][0] == '$'

  test "Encoding: command nested arrays flatten recursively":
    var req = RedisRequest.new
    req.initRedisRequest(nil)
    req.addCmd("T", [[1,2],[3,4]])
    check req.req.arr.len == 5
    check req.req.arr[4].str.get() == "4"

  test "Encoding: command args add and extend are equivalent for collections":
    var a = RedisRequest.new; a.initRedisRequest(nil)
    a.addCmd("DEL"); a.add(["k1","k2"])
    var b = RedisRequest.new; b.initRedisRequest(nil)
    b.addCmd("DEL"); b.extend(["k1","k2"])
    check a.req == b.req

  test "Decoding":
    proc server() {.async.} =
      var sock = newAsyncSocket()
      sock.setSockOpt(OptReuseAddr, true)
      sock.bindAddr(Port(16379))
      sock.listen()
      let client = await sock.accept()
      let str = await client.recvLine()
      check(str == "PING")
      var req = RedisRequest.new
      req.initRedisRequest(nil)
      req.addCmd("PONG")
      let data = req.req.prepareRequest()
      var line = data.join("\r\L")
      line = line & "\r\L"
      await client.send(line)
      await sleepAsync(1000)
      client.close()
      sock.close()
    
    proc main() {.async.} =
      asyncCheck(server())
      var connection = newRedis("localhost", 16379, 0, poolsize=2, timeout=5000)
      try:
        await connection.connect()
        connection.withRedis:
          await redis.sendLine(@["PING"])
          let replMsg = await redis.parseResponse()
          check(replMsg.arr[0].str == "PONG".option)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(main())

  proc wire(parts: seq[string]): string = parts.join("\r\n") & "\r\n"

  test "prepareRequest RESP3 encodingBOOL":
    check wire(RedisMessage(kind: REDIS_MESSAGE_BOOL, boolean: true).prepareRequest()) == "#t\r\n"
    check wire(RedisMessage(kind: REDIS_MESSAGE_BOOL, boolean: false).prepareRequest()) == "#f\r\n"

  test "prepareRequest RESP3 encoding SET":
    var s = RedisMessage(kind: REDIS_MESSAGE_SET, hashSet: initHashSet[RedisMessage]())
    s.hashSet.incl(RedisMessage(kind: REDIS_MESSAGE_STRING, str: some("a")))
    check wire(s.prepareRequest()) == "~1\r\n$1\r\na\r\n"

  test "prepareRequest RESP3 encoding MAP":
    var m = RedisMessage(kind: REDIS_MESSAGE_MAP, map: initTable[string, RedisMessage]())
    m.map["k"] = RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 1)
    check wire(m.prepareRequest()) == "%1\r\n$1\r\nk\r\n:1\r\n"


  proc parseWire(payload: string): Future[RedisMessage] {.async.} =
    var listener = newAsyncSocket(buffered = false)
    listener.setSockOpt(OptReuseAddr, true)
    listener.bindAddr(Port(16380))
    listener.listen()
    proc serve() {.async.} =
      let client = await listener.accept()
      await client.send(payload)
      client.close()
    asyncCheck serve()
    var pool = newRedis("localhost", 16380, 0, poolsize = 1, timeout = 2000)
    await pool.connect()
    let conn = await pool.acquire()
    try:
      result = await conn.parseResponse()
    finally:
      conn.release()
      await pool.close()
      listener.close()

  test "parse BOOL valid":
    let m = waitFor parseWire("#t\r\n")
    check m.kind == REDIS_MESSAGE_BOOL and m.boolean

  test "parse BOOL invalid single char":
    expect RedisProtocolError:
      discard waitFor parseWire("#x\r\n")

  test "parse BOOL invalid length":
    expect RedisProtocolError:
      discard waitFor parseWire("#tt\r\n")

  test "parse empty line":
    expect RedisProtocolError:
      discard waitFor parseWire("\r\n")

  test "parse VERB missing separator":
    expect RedisProtocolError:
      discard waitFor parseWire("=4\r\nbad!\r\n")

  test "parse VERB valid":
    let m = waitFor parseWire("=9\r\ntxt:hello\r\n")
    check m.kind == REDIS_MESSAGE_VERB and m.str.get() == "txt:hello"

  test "equal values equal and hash equally":
    let a = RedisMessage(kind: REDIS_MESSAGE_STRING, str: some("x"))
    let b = RedisMessage(kind: REDIS_MESSAGE_STRING, str: some("x"))
    check a == b
    check hash(a) == hash(b)

  test "different kinds not equal":
    let i = RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 1)
    let s = RedisMessage(kind: REDIS_MESSAGE_STRING, str: some("1"))
    check i != s

  test "SET dedups equal values":
    var hs = initHashSet[RedisMessage]()
    hs.incl(RedisMessage(kind: REDIS_MESSAGE_STRING, str: some("x")))
    hs.incl(RedisMessage(kind: REDIS_MESSAGE_STRING, str: some("x")))
    check hs.len == 1
    check hs.contains(RedisMessage(kind: REDIS_MESSAGE_STRING, str: some("x")))

  test "nested array equality":
    let a = RedisMessage(kind: REDIS_MESSAGE_ARRAY, arr: @[RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 1)])
    let b = RedisMessage(kind: REDIS_MESSAGE_ARRAY, arr: @[RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 1)])
    check a == b and hash(a) == hash(b)

  test "MAP hash order-independent":
    var m1 = RedisMessage(kind: REDIS_MESSAGE_MAP, map: initTable[string, RedisMessage]())
    m1.map["a"] = RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 1)
    m1.map["b"] = RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 2)
    var m2 = RedisMessage(kind: REDIS_MESSAGE_MAP, map: initTable[string, RedisMessage]())
    m2.map["b"] = RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 2)
    m2.map["a"] = RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: 1)
    check m1 == m2 and hash(m1) == hash(m2)
