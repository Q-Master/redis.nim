import std/[unittest, strutils]
import asyncdispatch
import asyncredis/asyncredis


const REDIS_HOST {.strdefine.} = "localhost"

echo "REDIS_HOST: ", REDIS_HOST

suite "Redis connection":
  setup:
    discard

  test "Simple connect/disconnect":
    proc testConnection() {.async.} =
      var connection = newRedis(REDIS_HOST, 6379, 0, poolsize=2, timeout=5000)
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
    proc testConnection() {.async.} =
      var connection = newRedis(REDIS_HOST, 6379, 0, poolsize=2, timeout=5000)
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

  test "Reconnect after CLIENT KILL":
    proc testConnection() {.async.} =
      var pool = newRedis(REDIS_HOST, 6379, 0, poolsize = 1, timeout = 2000)
      check((await pool.ping()) == "PONG")
      let id = await pool.clientID()

      let admin = newRedis(REDIS_HOST, 6379, 0, poolsize = 1, timeout = 2000)
      discard await admin.cmd("CLIENT", "KILL", "ID", $id)
      await admin.close()
      expect RedisConnectionError:
        discard await pool.ping()
      check((await pool.ping()) == "PONG")
      await pool.close()
    waitFor(testConnection())

  test "Dirty buffer after read timeout":
    proc testConnection() {.async.} =
      let pool = newRedis(REDIS_HOST, 6379, 0, poolsize = 1, timeout = 300)
      expect RedisConnectionError:
        discard await pool.cmd("BLPOP", "no_such_key_timeout_test", "0")
      check((await pool.ping()) == "PONG")
      await pool.close()
    waitFor(testConnection())
