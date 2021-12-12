import unittest
import asyncdispatch
import redis/private/connection
import redis/private/exceptions

suite "Redis connection":
  setup:
    discard

  test "Simple connect/disconnect":
    proc testConnection() {.async} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        await connection.connect()
        connection.withRedis:
          await redis.sendLine(@["PING"])
          let replStr = await redis.readLine()
          check(replStr == "+PONG")
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testConnection())

  test "Simple connect/disconnect using with statement":
    proc testConnection() {.async} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        await connection.connect()
        connection.withRedis:
          await redis.sendLine(@["PING"])
          let replStr = await redis.readLine()
          check(replStr == "+PONG")
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testConnection())
