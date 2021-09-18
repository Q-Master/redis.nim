import unittest
import std/[asyncdispatch, asyncnet, strutils, options]
import redis/private/commands/cmd
import redis/private/connection
import redis/private/proto

suite "Protocol":
  setup:
    discard

  test "Encoding":
    var req = RedisRequest.new
    req.initRedisRequest(nil)
    req.addCmd("PONG")
    check(req.req.kind == REDIS_MESSAGE_STRING)
    check(req.req.str.get() == "PONG")
    req.addCmd("TEST", 1, 1.0, "string", true, [1,2,3,4,5])
    check(req.req.kind == REDIS_MESSAGE_ARRAY)
    check(req.req.arr[0].str.get() == "TEST")
    check(req.req.arr[1].kind == REDIS_MESSAGE_INTEGER)
    check(req.req.arr[1].integer == 1)
    check(req.req.arr[2].kind == REDIS_MESSAGE_DOUBLE)
    check(req.req.arr[2].double == 1.0)
    check(req.req.arr[3].kind == REDIS_MESSAGE_STRING)
    check(req.req.arr[3].str.get() == "string")
    check(req.req.arr[4].kind == REDIS_MESSAGE_BOOL)
    check(req.req.arr[4].boolean == true)
    check(req.req.arr[5].kind == REDIS_MESSAGE_ARRAY)
    check(req.req.arr[5].arr[0].integer == 1)
    check(req.req.arr[5].arr[1].integer == 2)
    check(req.req.arr[5].arr[2].integer == 3)
    check(req.req.arr[5].arr[3].integer == 4)
    check(req.req.arr[5].arr[4].integer == 5)
    echo $req.req
  
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
      var pool = newRedisPool("localhost", 16379, 0, poolsize=2)
      var connection = await pool.acquire(5000)
      await connection.sendLine(@["PING"])
      let resp = await connection.parseResponse()
      check(resp.str[] == "PONG")
      connection.release()
    
    waitFor(main())