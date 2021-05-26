import unittest
import std/[asyncdispatch, asyncnet, strutils]
import redis/private/connection
import redis/private/proto
import redis/private/exceptions

suite "Protocol":
  setup:
    discard

  test "Encoding":
    var data = encodeCommand("PONG")
    check(data.messageType == REDIS_MESSAGE_STRING)
    check(data.str[] == "PONG")
    data = encodeCommand("TEST", 1)
    check(data.messageType == REDIS_MESSAGE_ARRAY)
    check(data.arr[0].str[] == "TEST")
    check(data.arr[1].integer == 1)

  test "Decoding":
    proc server() {.async.} =
      var sock = newAsyncSocket()
      sock.setSockOpt(OptReuseAddr, true)
      sock.bindAddr(Port(6379))
      sock.listen()
      let client = await sock.accept()
      let str = await client.recvLine()
      check(str == "PING")
      let data = encodeCommand("PONG").prepareRequest()
      var line = data.join("\r\L")
      line = line & "\r\L"
      await client.send(line)
      await sleepAsync(1000)
      client.close()
      sock.close()
    
    proc main() {.async.} =
      asyncCheck(server())
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      var connection = await pool.acquire(5000)
      await connection.sendLine(@["PING"])
      let resp = await connection.parseResponse()
      check(resp.str[] == "PONG")
      connection.release()
    
    waitFor(main())