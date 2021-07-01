import unittest
import asyncdispatch
import redis/redis

suite "Redis commands":
  setup:
    discard

  test "Connection commands":
    const PING_TEST_MSG = "Test Message"
    proc testConnection() {.async} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        let pingResp = await connection.ping()
        assert(pingResp == "")
        let pingRespWithString = await connection.ping(PING_TEST_MSG)
        assert(pingRespWithString == PING_TEST_MSG)
        let clResponse: seq[ClientInfo] = await connection.clientList()
        assert(clResponse[0].cmd == "client")
        assert(clResponse.len == 1)
        let ciResponse: ClientInfo = await connection.clientInfo()
        assert(ciResponse.cmd == "client")
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testConnection())
