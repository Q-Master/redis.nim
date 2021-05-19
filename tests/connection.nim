import unittest
import asyncdispatch
import redis/private/connection
import redis/private/exceptions

suite "Redis connection":
  setup:
    discard

  test "Simple connect/disconnect":
    proc testConnection() {.async} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        await connection.sendLine(@["PING"])
        echo await connection.readLine()
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
      await pool.close()
    waitFor(testConnection())

  test "Simple connect/disconnect using with statement":
    proc testConnection() {.async} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        pool.withRedis 5000:
          await redis.sendLine(@["PING"])
          echo await redis.readLine()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
      await pool.close()
    waitFor(testConnection())
