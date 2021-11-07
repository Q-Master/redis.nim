import unittest
import std/[asyncdispatch, times, options, tables]
import redis/redis

suite "Redis commands":
  const KEY1 = "KEY1"
  const KEY2 = "KEY2"
  const KEY3 = "KEY3"
  const KEY4 = "KEY4"
  const KEY_NONEXISTING = "NONEXISTING"
  const TEST_STRING = "Test String"
  const TEST_STRING_1 = "New Test String"
    
  setup:
    proc killAll() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        await connection.connect()
        let x {.used.} = await connection.flushAll(REDIS_FLUSH_SYNC)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(killAll())

  test "Connection commands":
    const PING_TEST_MSG = "Test Message"
    const TEST_USER_NAME = "redis.nim"
    proc testConnection() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        await connection.connect()
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
        check(clResponse.len == 2)
        let ciResponse: ClientInfo = await connection.clientInfo()
        check(ciResponse.cmd == "client")
        let redirResponse = await connection.clientGetRedir()
        check(redirResponse == -1)
        check(clResponse[0].redir == redirResponse)
        check(ciResponse.redir == redirResponse)
        let idResponse = await connection.clientID()
        check(idResponse == clResponse[0].id)
        check(idResponse == ciResponse.id)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testConnection())

  test "Strings commands":
    const CHECK_STRING = "Test stringS"
    proc testStrings() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        var boolRepl: bool
        var optStrRepl: Option[string]
        var intRepl: int64
        var floatRepl: float
        var optStrReplArray: seq[Option[string]]
        await connection.connect()
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
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testStrings())

  test "Keys commands":
    const KEYS_PATTERN = "KEY[1-5]"
    proc testKeys() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        var boolRepl: bool
        var optStrRepl: Option[string]
        var intRepl: int64
        var strReplArray: seq[string]
        await connection.connect()
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
        check(strReplArray[0] == KEY1 or strReplArray[0] == KEY2)
        check(strReplArray[1] == KEY2 or strReplArray[1] == KEY1)
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
        # TOUCH key [key ...]
        boolRepl = await connection.mSet((KEY1, TEST_STRING), (KEY2, TEST_STRING), (KEY3, TEST_STRING))
        intRepl = await connection.touch(KEY1, KEY2, KEY3, KEY_NONEXISTING)
        check(intRepl == 3)
        # UNLINK key [key ...]
        boolRepl = await connection.mSet((KEY1, TEST_STRING), (KEY2, TEST_STRING), (KEY3, TEST_STRING), (KEY4, TEST_STRING))
        intRepl = await connection.unlink(KEY1, KEY2, KEY3, KEY4, KEY_NONEXISTING)
        check(intRepl == 4)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testKeys())

  test "Lists commands":
    proc testLists() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        var boolRepl: bool
        var optStrRepl: Option[string]
        var intRepl: int64
        var optIntRepl: Option[int64]
        var intReplArray: seq[int64]
        var strReplArray: seq[string]
        await connection.connect()
        # RPUSH key element [element ...]
        intRepl = await connection.rPush(KEY1, 1, 2, 3, 4, 5, 6)
        check(intRepl == 6)
        # RPUSHX key element [element ...]
        intRepl = await connection.rPushX(KEY1, 7, 8)
        check(intRepl == 8)
        intRepl = await connection.rPushX(KEY2, 1, 2, 3, 4, 5, 6)
        check(intRepl == 0)
        # LPUSH key element [element ...]
        intRepl = await connection.lPush(KEY1, 1, 2)
        check(intRepl == 10)
        # LPUSHX key element [element ...]
        intRepl = await connection.lPushX(KEY1, 7, 8)
        check(intRepl == 12)
        intRepl = await connection.lPushX(KEY2, 1, 2, 3, 4, 5, 6)
        check(intRepl == 0)
        # LRANGE key start stop
        strReplArray = await connection.lRange(KEY1, (4 .. 8))
        check(strReplArray == @["1", "2", "3", "4", "5"])
        # LREM key count element
        intRepl = await connection.lRem(KEY1, "1")
        check(intRepl == 2)
        # LINDEX key index
        optStrRepl = await connection.lIndex(KEY1, 0)
        check(optStrRepl == "8".option)
        optStrRepl = await connection.lIndex(KEY1, 20)
        check(optStrRepl.isNone == true)
        # LSET key index element
        intRepl = await connection.del(KEY1)
        intRepl = await connection.rPush(KEY1, 1, 2, 3, 4, 5, 6)
        boolRepl = await connection.lSet(KEY1, "TEST", 1)
        optStrRepl = await connection.lIndex(KEY1, 1)
        check(optStrRepl == "TEST".option)
        # LTRIM key start stop 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.rPush(KEY1, 1, 2, 3, 4, 5, 6)
        boolRepl = await connection.lTrim(KEY1, (0 .. 2))
        strReplArray = await connection.lRange(KEY1, (0 .. -1))
        check(strReplArray == @["1", "2", "3"])
        # RPOP key [count]
        intRepl = await connection.del(KEY1)
        intRepl = await connection.rPush(KEY1, 1, 2, 3, 4, 5, 6)
        optStrRepl = await connection.rPop(KEY1)
        check(optStrRepl == "6".option)
        strReplArray = await connection.rPop(KEY1, 2)
        check(strReplArray == @["5", "4"])
        optStrRepl = await connection.rPop(KEY2)
        check(optStrRepl.isNone)
        # LPOP key [count]
        intRepl = await connection.del(KEY1)
        intRepl = await connection.lPush(KEY1, 1, 2, 3, 4, 5, 6)
        optStrRepl = await connection.lPop(KEY1)
        check(optStrRepl == "6".option)
        strReplArray = await connection.lPop(KEY1, 2)
        check(strReplArray == @["5", "4"])
        optStrRepl = await connection.lPop(KEY2)
        check(optStrRepl.isNone)
        # LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
        intRepl = await connection.del(KEY1)
        intRepl = await connection.lPush(KEY1, "C", "C", "C", "A", "B", "C", "D")
        optIntRepl = await connection.lPos(KEY1, "C")
        check(optIntRepl == 1.int64.option)
        optIntRepl = await connection.lPos(KEY1, "C").rank(3)
        check(optIntRepl == 5.int64.option)
        optIntRepl = await connection.lPos(KEY1, "C").rank(3).maxLen(2)
        check(optIntRepl.isNone)
        intReplArray = await connection.lPos(KEY1, "C").count(4)
        check(intReplArray == @[1.int64, 4.int64, 5.int64, 6.int64])
        intReplArray = await connection.lPos(KEY1, "C").count(4).rank(3)
        check(intReplArray == @[5.int64, 6.int64])
        intReplArray = await connection.lPos(KEY1, "C").count(4).maxLen(5)
        check(intReplArray == @[1.int64, 4.int64])
        intReplArray = await connection.lPos(KEY1, "C").count(4).maxLen(1)
        check(intReplArray.len == 0)
        # LLEN key 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.rPush(KEY1, 1, 2, 3, 4, 5, 6)
        intRepl = await connection.lLen(KEY1)
        check(intRepl == 6)
        intRepl = await connection.lLen(KEY2)
        check(intRepl == 0)
        # LMOVE source destination LEFT|RIGHT LEFT|RIGHT 
        optStrRepl = await connection.lMove(KEY1, MOVE_LEFT, KEY2, MOVE_RIGHT)
        check(optStrRepl == "1".option)
        intRepl = await connection.lLen(KEY2)
        check(intRepl == 1)
        # LINSERT key BEFORE|AFTER pivot element 
        intRepl = await connection.del(KEY1, KEY2)
        intRepl = await connection.rPush(KEY1, 1, 2, 3, 4, 5, 6)
        intRepl = await connection.lInsert(KEY1, "2", "10", INDEX_BEFORE)
        check(intRepl == 7)
        strReplArray = await connection.lRange(KEY1, (0 .. -1))
        check(strReplArray == @["1", "10", "2", "3", "4", "5", "6"])
        # RPOPLPUSH source destination (deprecated since 6.2)
        optStrRepl = await connection.rPoplPush(KEY1, KEY2)
        check(optStrRepl == "6".option)
        intRepl = await connection.lLen(KEY2)
        check(intRepl == 1)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testLists())

  test "Hashes commands":
    proc testHashes() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        var boolRepl: bool
        var optStrRepl: Option[string]
        var intRepl: int64
        var floatRepl: float
        var optIntRepl: Option[int64]
        var intReplArray: seq[int64]
        var strReplArray: seq[string]
        var optStrReplArray: seq[Option[string]]
        var tableRepl: Table[string, string]
        await connection.connect()
        # HSET key field value [field value ...] 
        intRepl = await connection.hSet(KEY1, ("k", 1), ("v", 2))
        check(intRepl == 2)
        intRepl = await connection.hSet(KEY1, {"a": 1, "b": 2}.toTable())
        check(intRepl == 2)
        # HSETNX key field value 
        boolRepl = await connection.hSetNX(KEY1, "k", 1)
        check(boolRepl == false)
        boolRepl = await connection.hSetNX(KEY1, "t", 3)
        check(boolRepl == true)
        # HSTRLEN key field 
        intRepl = await connection.hStrLen(KEY1, "k")
        check(intRepl == 1)
        intRepl = await connection.hStrLen(KEY1, "e")
        check(intRepl == 0)
        # HVALS key 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.hSet(KEY1, {"a": 1, "b": 2, "c": 3}.toTable())
        strReplArray = await connection.hVals(KEY1)
        check(strReplArray == @["1", "2", "3"])
        # HGET key field 
        optStrRepl = await connection.hGet(KEY1, "a")
        check(optStrRepl == "1".option)
        optStrRepl = await connection.hGet(KEY1, "e")
        check(optStrRepl.isNone)
        # HGETALL key 
        tableRepl = await connection.hGetAll(KEY1)
        check(tableRepl == {"a": "1", "b": "2", "c": "3"}.toTable())
        # HEXISTS key field 
        boolRepl = await connection.hExists(KEY1, "a")
        check(boolRepl == true)
        boolRepl = await connection.hExists(KEY1, "f")
        check(boolRepl == false)
        boolRepl = await connection.hExists(KEY2, "f")
        check(boolRepl == false)
        # HDEL key field [field ...] 
        intRepl = await connection.hDel(KEY1, "a", "b", "z")
        check(intRepl == 2)
        tableRepl = await connection.hGetAll(KEY1)
        check(tableRepl == {"c": "3"}.toTable())
        # HKEYS key 
        strReplArray = await connection.hKeys(KEY1)
        check(strReplArray == @["c"])
        # HLEN key 
        intRepl = await connection.hLen(KEY1)
        check(intRepl == 1)
        # HMSET key field value [field value ...] 
        intRepl = await connection.del(KEY1)
        boolRepl = await connection.hmSet(KEY1, ("k", 1), ("v", 2))
        check(boolRepl == true)
        boolRepl = await connection.hmSet(KEY1, {"a": 1, "b": 2}.toTable())
        check(boolRepl == true)
        # HMGET key field [field ...] 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.hSet(KEY1, {"a": 1, "b": 2, "c": 3}.toTable())
        optStrReplArray = await connection.hmGet(KEY1, "a", "b", "d")
        check(optStrReplArray == @["1".option, "2".option, string.none])
        # HRANDFIELD key [count [WITHVALUES]] 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.hSet(KEY1, {"a": 1, "b": 2, "c": 3}.toTable())
        optStrRepl = await connection.hRandField(KEY1)
        check(optStrRepl in ["a".option, "b".option, "c".option])
        optStrRepl = await connection.hRandField(KEY2)
        check(optStrRepl.isNone)
        strReplArray = await connection.hRandField(KEY1).count(2)
        check(strReplArray.len == 2)
        tableRepl = await connection.hRandField(KEY1).count(2).withValues()
        check(tableRepl.len == 2)
        # HINCRBY key field increment 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.hSet(KEY1, ("a", 1))
        intRepl = await connection.hIncrBy(KEY1, "a", 2)
        check(intRepl == 3)
        # HINCRBYFLOAT key field increment 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.hSet(KEY1, ("a", 1.5))
        floatRepl = await connection.hIncrBy(KEY1, "a", 0.2)
        check(floatRepl == 1.7)
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testHashes())
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

        let keysCur = connection.scan()
        asyncFor key in keysCur:
          echo key
]#
