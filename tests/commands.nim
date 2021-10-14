import unittest
import std/[asyncdispatch, times, options]
import redis/redis

suite "Redis commands":
  setup:
    proc killAll() {.async.} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        let x {.used.} = await connection.flushAll(REDIS_FLUSH_SYNC).execute()
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(killAll())

  test "Connection commands":
    const PING_TEST_MSG = "Test Message"
    const TEST_USER_NAME = "redis.nim"
    proc testConnection() {.async.} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        let pingResp = await connection.ping().execute()
        check(pingResp == "PONG")
        let echoResp = await connection.echo(PING_TEST_MSG).execute()
        check(echoResp == PING_TEST_MSG)
        let pingRespWithString = await connection.ping(PING_TEST_MSG.option).execute()
        check(pingRespWithString == PING_TEST_MSG)
        var nameResp = await connection.clientGetName().execute()
        check(nameResp.isSome != true)
        var boolRepl: bool = await connection.clientSetName(TEST_USER_NAME).execute()
        check(boolRepl == true)
        nameResp = await connection.clientGetName().execute()
        check(nameResp.get("") == TEST_USER_NAME)
        let clResponse: seq[ClientInfo] = await connection.clientList().execute()
        check(clResponse[0].cmd == "client")
        check(clResponse.len == 1)
        let ciResponse: ClientInfo = await connection.clientInfo().execute()
        check(ciResponse.cmd == "client")
        let redirResponse = await connection.clientGetRedir().execute()
        check(redirResponse == -1)
        check(clResponse[0].redir == redirResponse)
        check(ciResponse.redir == redirResponse)
        let idResponse = await connection.clientID().execute()
        check(idResponse == clResponse[0].id)
        check(idResponse == ciResponse.id)
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testConnection())

  test "Strings commands":
    const KEY1 = "KEY1"
    const KEY2 = "KEY2"
    const KEY3 = "KEY3"
    const KEY4 = "KEY4"
    const TEST_STRING = "Test String"
    const TEST_STRING_1 = "New Test String"
    proc testStrings() {.async.} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        var boolRepl: bool
        var strRepl: string
        var optStrRepl: Option[string]
        var intRepl: int64
        var floatRepl: float
        # SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
        boolRepl = await connection.set(KEY1, TEST_STRING).execute()
        check(boolRepl == true)
        boolRepl = await connection.set(KEY1, TEST_STRING_1).nx().execute()
        check(boolRepl == false)
        boolRepl = await connection.set(KEY2, TEST_STRING).nx().execute()
        check(boolRepl == true)
        boolRepl = await connection.set(KEY2, TEST_STRING_1).xx().execute()
        check(boolRepl == true)
        boolRepl = await connection.set(KEY3, TEST_STRING).xx().execute()
        check(boolRepl == false)
        optStrRepl = await connection.set(KEY2, TEST_STRING).get().execute()
        check(optStrRepl.get("") == TEST_STRING_1)
        # GET key
        optStrRepl = await connection.get(KEY1).execute()
        check(optStrRepl == TEST_STRING.option)
        optStrRepl = await connection.get(KEY4).execute()
        check(optStrRepl.isSome == false)
        # APPEND key value
        intRepl = await connection.append(KEY1, " 1").execute()
        check(intRepl == TEST_STRING.len + 2)
        optStrRepl = await connection.get(KEY1).execute()
        check(optStrRepl == (TEST_STRING & " 1").option)
        # DECR key
        boolRepl = await connection.set(KEY1, 5).execute()
        intRepl = await connection.decr(KEY1).execute()
        check(intRepl == 4)
        # DECRBY key decrement 
        boolRepl = await connection.set(KEY1, 5).execute()
        intRepl = await connection.decrBy(KEY1, 4).execute()
        check(intRepl == 1)
        connection.release()
        # GETDEL key
        optStrRepl = await connection.getDel(KEY1).execute()
        check(optStrRepl == "1".option)
        optStrRepl = await connection.getDel(KEY1).execute()
        check(optStrRepl.isSome == false)
        # GETRANGE key start end 

      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testStrings())

#[

        var replSize = await connection.append("KEY1", "Test")
        assert(replSize == 4)
        replSize = await connection.decr("KEY2")
        assert(replSize == 9)
        replSize = await connection.decrBy("KEY2", 2)
        assert(replSize == 7)
        var replGet = await connection.get("KEY1")
        assert(replGet.get() == "Test")
        replGet = await connection.get("NONEXISTING")
        assert(replGet.isNone())
        replGet = await connection.getDel("KEY1")
        assert(replGet.get() == "Test")
        replGet = await connection.get("KEY1")
        assert(replGet.isNone())
        let expTime = now()+2.seconds+500.milliseconds
        replGet = await connection.getEx("KEY1").ex(initDuration(seconds=60)).execute()
        replGet = await connection.getEx("KEY1").exAt(expTime.toTime()).execute()
        replGet = await connection.getEx("KEY1").exAt(expTime).execute()
        replGet = await connection.getEx("KEY1").px(initDuration(seconds=60)).execute()
        replGet = await connection.getEx("KEY1").pxAt(expTime.toTime()).execute
        replGet = await connection.getEx("KEY1").pxAt(expTime).execute()
        replGet = await connection.getRange("KEY1", 0 .. 1)
        assert(replGet.get() == "Te")
        replGet = await connection.getSet("KEY1", "New Test")
        assert(replGet.get() == "Test")
        replGet = await connection.get("KEY1")
        assert(replGet.get() == "New Test")
        replSize = await connection.incr("KEY2")
        assert(replSize == 8)
        replSize = await connection.incrBy("KEY2", 2)
        assert(replSize == 10)
        var replSizeFloat = await connection.incrBy("KEY2", 2.5)
        assert(replSizeFloat == 12.5)
        let mgetRepl: seq[Option[string]] = await connection.mget("KEY1", "NONEXISTENT")
        assert(mgetRepl[0].get() == "New Test")
        assert(mgetRepl[1].isNone())
        var msetRepl = await connection.mset({"KEY1": "Test"}, {"KEY2": 1})
        assert(msetRepl == true)
        msetRepl = await connection.msetNx({"KEY1": "Test"}, {"KEY2": 1})
        assert(msetRepl == true)
        msetRepl = await connection.msetNx({"KEY1": "Test"}, {"NONEXISTENT": "Error"})
        assert(msetRepl == false)
        msetRepl = await connection.setEx("KEY1", initDuration(seconds = 60), "Test")
        msetRepl = await connection.setEx("KEY1", expTime.toTime(), "Test")
        msetRepl = await connection.setEx("KEY1", expTime, "Test")
        msetRepl = await connection.psetEx("KEY1", initDuration(seconds = 60), "Test")
        msetRepl = await connection.psetEx("KEY1", expTime.toTime(), "Test")
        msetRepl = await connection.psetEx("KEY1", expTime, "Test")
        assert(msetRepl == true)
        msetRepl = await connection.setVal("KEY1", "Test").execute()
        assert(msetRepl == true)
        replGet = await connection.setVal("KEY1", "Test1").get().execute()
        assert(replGet.get() == "Test")
        msetRepl = await connection.setNx("KEY1", "TEST")
        assert(msetRepl == false)
        replSize = await connection.setRange("KEY1", "2", 4)
        assert(replSize == 5)
        replSize = await connection.strLen("KEY1")
        assert(replSize == 5)
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
        assert(deletedKeys == 3)
        # exists 1 key
        var existedKeys: int = await connection.exists("KEY1")
        assert(existedKeys == 1)
        # exists more keys
        existedKeys = await connection.exists("KEY1", "KEY2", "KEY3")
        assert(existedKeys > 0)
        var expireRes = await connection.expire("KEY1", initDuration(seconds=1))
        assert(expireRes == true)
        await sleepAsync(2000)
        existedKeys = await connection.exists("KEY1")
        assert(existedKeys == 0)
        var expTime = now()+2.seconds
        expireRes = await connection.expireAt("KEY1", expTime)
        assert(expireRes == true)
        expireRes = await connection.expireAt("KEY2", expTime.toTime())
        assert(expireRes == true)
        var expTimeRes = await connection.expireTime("KEY1")
        assert(expTimeRes == expTime.toTime())
        var ttl = await connection.ttl("KEY1")
        assert(ttl <= initDuration(seconds = 2))
        await sleepAsync(3000)
        existedKeys = await connection.exists("KEY1")
        assert(existedKeys == 0)
        let keysRes = await connection.keys("KEY*")
        assert(keysRes.len == 3)
        let moveRes = await connection.move("KEY1", 2)
        assert(moveRes == true)
        let persRes = await connection.persist("KEY1")
        assert(persRes == true)
        expireRes = await connection.pexpire("KEY1", initDuration(milliseconds=1500))
        assert(persRes == true)
        expTime = now()+2.seconds+500.milliseconds
        expireRes = await connection.pexpireAt("KEY1", expTime)
        assert(expireRes == true)
        expireRes = await connection.pexpireAt("KEY2", expTime.toTime())
        assert(expireRes == true)
        expTimeRes = await connection.pexpireTime("KEY1")
        assert(expTimeRes == expTime.toTime())
        expTimeRes = await connection.pexpireTime("KEY2")
        assert(expTimeRes == expTime.toTime())
        ttl = await connection.pTTL("KEY1")
        assert(ttl <= initDuration(milliseconds = 500))
        let key = await connection.randomKey()
        assert(key.get("") == "KEY1")
        var ren = await connection.rename("KEY1", "KEY11")
        assert(ren == true)
        ren = await connection.renameNX("KEY1", "KEY11")
        assert(ren == true)
        let keysCur = connection.scan()
        asyncFor key in keysCur:
          echo key
        let keys = await connection.sort("KEY1")
        assert(keys.len() == 10)
        var count = await connection.sortAndStore("KEY1", "KEY2")
        assert(count == 10)
        count = await connection.touch("KEY1", "KEY2")
        assert(count == 2)
        deletedKeys = await connection.unlink("KEY1")
        assert(deletedKeys == 1)
        # del more keys
        deletedKeys = await connection.unlink("KEY1", "KEY2", "KEY3")
        assert(deletedKeys == 3)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testKeys())
]#
