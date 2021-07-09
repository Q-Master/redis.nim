import unittest
import std/[asyncdispatch, times]
import redis/redis

suite "Redis commands":
  setup:
    discard

  test "Connection commands":
    const PING_TEST_MSG = "Test Message"
    const TEST_USER_NAME = "redis.nim"
    proc testConnection() {.async} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        let pingResp = await connection.ping()
        assert(pingResp == "")
        let echoResp = await connection.echo(PING_TEST_MSG)
        assert(echoResp == PING_TEST_MSG)
        let pingRespWithString = await connection.ping(PING_TEST_MSG)
        assert(pingRespWithString == PING_TEST_MSG)
        var nameResp = await connection.clientGetName()
        assert(nameResp.isNil == true)
        await connection.clientSetName(TEST_USER_NAME)
        nameResp = await connection.clientGetName()
        assert(nameResp[] == TEST_USER_NAME)
        let clResponse: seq[ClientInfo] = await connection.clientList()
        assert(clResponse[0].cmd == "client")
        assert(clResponse.len == 1)
        let ciResponse: ClientInfo = await connection.clientInfo()
        assert(ciResponse.cmd == "client")
        let redirResponse = await connection.clientGetRedir()
        assert(redirResponse == -1)
        assert(clResponse[0].redir == redirResponse)
        assert(ciResponse.redir == redirResponse)
        let idResponse = await connection.clientID()
        assert(idResponse == clResponse[0].id)
        assert(idResponse == ciResponse.id)
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testConnection())

  test "Keys commands":
    proc testKeys() {.async} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        let copied = await connection.copy("source", "destination", replace = true)
        assert(copied == true)
        # del 1 key
        var deletedKeys: int = await connection.del("KEY1")
        assert(deletedKeys == 1)
        # del more keys
        deletedKeys = await connection.del("KEY1", "KEY2", "KEY3")
        assert(deletedKeys > 0)
        # exists 1 key
        var existedKeys: int = await connection.exists("KEY1")
        assert(existedKeys == 1)
        # exists more keys
        existedKeys = await connection.exists("KEY1", "KEY2", "KEY3")
        assert(existedKeys > 0)
        var expireRes = await connection.expire("KEY1", 1)
        assert(expireRes == true)
        await sleepAsync(2000)
        existedKeys = await connection.exists("KEY1")
        assert(existedKeys == 0)
        let expTime = now()+2.seconds
        expireRes = await connection.expireAt("KEY1", expTime)
        assert(expireRes == true)
        expireRes = await connection.expireAt("KEY2", expTime.toTime())
        assert(expireRes == true)
        let expTimeRes = await connection.expireTime("KEY1")
        assert(expTimeRes == expTime.toTime())
        await sleepAsync(3000)
        existedKeys = await connection.exists("KEY1")
        assert(existedKeys == 0)
        let keysRes = await connection.keys("KEY*")
        assert(keysRes.len == 3)
        let moveRes = await connection.move("KEY1", 2)
        assert(moveRes == true)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testKeys())