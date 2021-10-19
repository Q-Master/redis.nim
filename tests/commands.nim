import unittest
import std/[asyncdispatch, times, options]
import redis/redis

suite "Redis commands":
  const KEY1 = "KEY1"
  const KEY2 = "KEY2"
  const KEY3 = "KEY3"
  const KEY4 = "KEY4"
  const TEST_STRING = "Test String"
  const TEST_STRING_1 = "New Test String"
    
  setup:
    proc killAll() {.async.} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        let x {.used.} = await connection.flushAll(REDIS_FLUSH_SYNC)
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
        let pingResp = await connection.ping()
        check(pingResp == "PONG")
        let echoResp = await connection.echo(PING_TEST_MSG)
        check(echoResp == PING_TEST_MSG)
        let pingRespWithString = await connection.ping(PING_TEST_MSG.option)
        check(pingRespWithString == PING_TEST_MSG)
        var nameResp = await connection.clientGetName()
        check(nameResp.isSome != true)
        var boolRepl: bool = await connection.clientSetName(TEST_USER_NAME)
        check(boolRepl == true)
        nameResp = await connection.clientGetName()
        check(nameResp.get("") == TEST_USER_NAME)
        let clResponse: seq[ClientInfo] = await connection.clientList()
        check(clResponse[0].cmd == "client")
        check(clResponse.len == 1)
        let ciResponse: ClientInfo = await connection.clientInfo()
        check(ciResponse.cmd == "client")
        let redirResponse = await connection.clientGetRedir()
        check(redirResponse == -1)
        check(clResponse[0].redir == redirResponse)
        check(ciResponse.redir == redirResponse)
        let idResponse = await connection.clientID()
        check(idResponse == clResponse[0].id)
        check(idResponse == ciResponse.id)
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testConnection())

  test "Strings commands":
    const CHECK_STRING = "Test stringS"
    proc testStrings() {.async.} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        var boolRepl: bool
        var optStrRepl: Option[string]
        var intRepl: int64
        var floatRepl: float
        var optStrReplArray: seq[Option[string]]
        # SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
        boolRepl = await connection.set(KEY1, TEST_STRING)
        check(boolRepl == true)
        boolRepl = await connection.set(KEY1, TEST_STRING_1).nx()
        check(boolRepl == false)
        boolRepl = await connection.set(KEY2, TEST_STRING).nx()
        check(boolRepl == true)
        boolRepl = await connection.set(KEY2, TEST_STRING_1).xx()
        check(boolRepl == true)
        boolRepl = await connection.set(KEY3, TEST_STRING).xx()
        check(boolRepl == false)
        optStrRepl = await connection.set(KEY2, TEST_STRING).get()
        check(optStrRepl.get("") == TEST_STRING_1)
        # GET key
        optStrRepl = await connection.get(KEY1)
        check(optStrRepl == TEST_STRING.option)
        optStrRepl = await connection.get(KEY4)
        check(optStrRepl.isSome == false)
        # APPEND key value
        intRepl = await connection.append(KEY1, " 1")
        check(intRepl == TEST_STRING.len + 2)
        optStrRepl = await connection.get(KEY1)
        check(optStrRepl == (TEST_STRING & " 1").option)
        # DECR key
        boolRepl = await connection.set(KEY1, 5)
        intRepl = await connection.decr(KEY1)
        check(intRepl == 4)
        # DECRBY key decrement 
        boolRepl = await connection.set(KEY1, 5)
        intRepl = await connection.decrBy(KEY1, 4)
        check(intRepl == 1)
        connection.release()
        # GETDEL key
        optStrRepl = await connection.getDel(KEY1)
        check(optStrRepl == "1".option)
        optStrRepl = await connection.getDel(KEY1)
        check(optStrRepl.isSome == false)
        # GETRANGE key start end
        boolRepl = await connection.set(KEY1, TEST_STRING)
        optStrRepl = await connection.getRange(KEY1, (1 .. 3))
        check(optStrRepl == "est".option)
        # GETSET key value
        boolRepl = await connection.set(KEY1, TEST_STRING)
        optStrRepl = await connection.getSet(KEY1, TEST_STRING_1)
        check(optStrRepl == TEST_STRING.option)
        # INCR key
        boolRepl = await connection.set(KEY1, 5)
        intRepl = await connection.incr(KEY1)
        check(intRepl == 6)
        # INCRBY key increment
        intRepl = await connection.incrBy(KEY1, 4)
        check(intRepl == 10)
        # INCRBYFLOAT key increment
        floatRepl = await connection.incrBy(KEY1, 1.5)
        check(floatRepl == 11.5)
        # MGET key [key ...]
        boolRepl = await connection.set(KEY1, TEST_STRING)
        boolRepl = await connection.set(KEY2, TEST_STRING_1)
        optStrReplArray = await connection.mGet(KEY1, KEY2, KEY3)
        check(optStrReplArray.len == 3)
        check(optStrReplArray[0] == TEST_STRING.option)
        check(optStrReplArray[1] == TEST_STRING_1.option)
        check(optStrReplArray[2].isNone)
        # MSET key value [key value ...] 
        boolRepl = await connection.mSet((KEY1, TEST_STRING_1), (KEY2, TEST_STRING))
        optStrRepl = await connection.getDel(KEY1)
        check(optStrRepl == TEST_STRING_1.option)
        optStrRepl = await connection.getDel(KEY2)
        check(optStrRepl == TEST_STRING.option)
        # MSETNX key value [key value ...]
        boolRepl = await connection.mSetNX((KEY1, TEST_STRING), (KEY2, TEST_STRING_1))
        check(boolRepl == true)
        optStrRepl = await connection.get(KEY1)
        check(optStrRepl == TEST_STRING.option)
        optStrRepl = await connection.getDel(KEY2)
        check(optStrRepl == TEST_STRING_1.option)
        boolRepl = await connection.mSetNX((KEY1, TEST_STRING_1), (KEY2, TEST_STRING))
        check(boolRepl == false)
        optStrRepl = await connection.get(KEY1)
        check(optStrRepl == TEST_STRING.option)
        optStrRepl = await connection.get(KEY2)
        check(optStrRepl.isNone)
        # SETNX key value
        boolRepl = await connection.setNX(KEY1, TEST_STRING_1)
        check(boolRepl == false)
        boolRepl = await connection.setNX(KEY2, TEST_STRING_1)
        check(boolRepl == true)
        # SETRANGE key offset value 
        intRepl = await connection.setRange(KEY1, "stringS", 5)
        check(intRepl == CHECK_STRING.len)
        optStrRepl = await connection.get(KEY1)
        check(optStrRepl == CHECK_STRING.option)
        # STRLEN key
        intRepl = await connection.strLen(KEY1)
        check(intRepl == CHECK_STRING.len)
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testStrings())

  test "Keys commands":
    const KEYS_PATTERN = "KEY[1-5]"
    proc testKeys() {.async.} =
      var pool = newRedisPool("localhost", 6379, 0, poolsize=2)
      try:
        var connection = await pool.acquire(5000)
        var boolRepl: bool
        var optStrRepl: Option[string]
        var intRepl: int64
        var floatRepl: float
        var strReplArray: seq[string]
        # DEL key [key ...] 
        boolRepl = await connection.set(KEY1, TEST_STRING)
        boolRepl = await connection.set(KEY2, TEST_STRING)
        intRepl = await connection.del(KEY1, KEY2, KEY3)
        check(intRepl == 2)
        # EXISTS key [key ...] 
        boolRepl = await connection.set(KEY1, TEST_STRING)
        boolRepl = await connection.set(KEY2, TEST_STRING)
        intRepl = await connection.exists(KEY1, KEY2, KEY3)
        check(intRepl == 2)
        # KEYS pattern 
        strReplArray = await connection.keys(KEYS_PATTERN)
        check(strReplArray.len == 2)
        check(strReplArray[0] == KEY1)
        check(strReplArray[1] == KEY2)
        # RANDOMKEY
        optStrRepl = await connection.randomKey()
        check(optStrRepl.isSome)
        # RENAME key newkey 
        boolRepl = await connection.rename(KEY1, KEY4)
        check(boolRepl == true)
        intRepl = await connection.exists(KEY1, KEY4)
        check(intRepl == 1)
        optStrRepl = await connection.get(KEY4)
        check(optStrRepl.isSome)
        check(optStrRepl == TEST_STRING.option)
        # RENAMENX key newkey 
        boolRepl = await connection.renameNX(KEY4, KEY1)
        check(boolRepl == true)
        boolRepl = await connection.renameNX(KEY1, KEY2)
        check(boolRepl == false)
        connection.release()
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await pool.close()
    waitFor(testKeys())
#[
        let expTime = now()+2.seconds+500.milliseconds
        replGet = await connection.getEx("KEY1").ex(initDuration(seconds=60)).execute()
        replGet = await connection.getEx("KEY1").exAt(expTime.toTime()).execute()
        replGet = await connection.getEx("KEY1").exAt(expTime).execute()
        replGet = await connection.getEx("KEY1").px(initDuration(seconds=60)).execute()
        replGet = await connection.getEx("KEY1").pxAt(expTime.toTime()).execute
        replGet = await connection.getEx("KEY1").pxAt(expTime).execute()
        msetRepl = await connection.setEx("KEY1", initDuration(seconds = 60), "Test")
        msetRepl = await connection.setEx("KEY1", expTime.toTime(), "Test")
        msetRepl = await connection.setEx("KEY1", expTime, "Test")
        msetRepl = await connection.psetEx("KEY1", initDuration(seconds = 60), "Test")
        msetRepl = await connection.psetEx("KEY1", expTime.toTime(), "Test")
        msetRepl = await connection.psetEx("KEY1", expTime, "Test")

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
