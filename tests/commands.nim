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
        check(clResponse.len >= 2)
        let ciResponse: ClientInfo = await connection.clientInfo()
        check(ciResponse.cmd == "client")
        let redirResponse = await connection.clientGetRedir()
        check(redirResponse == -1)
        check(clResponse[0].redir == redirResponse)
        check(ciResponse.redir == redirResponse)
        let idResponse = await connection.clientID()
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
  
  test "Sets commands":
    proc testSets() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        var boolRepl: bool
        var optStrRepl: Option[string]
        var intRepl: int64
        var strReplArray: seq[string]
        var boolReplArray: seq[bool]
        await connection.connect()
        # SADD key member [member ...]
        intRepl = await connection.sAdd(KEY1, 1, 2, 3, "4", 4)
        check(intRepl == 4)
        expect RedisCommandError:
          intRepl = await connection.sAdd(KEY1)
        # SMEMBERS key 
        strReplArray = await connection.sMembers(KEY1)
        check(strReplArray == @["1", "2", "3", "4"])
        # SISMEMBER key member
        boolRepl = await connection.sIsMember(KEY1, "1")
        check(boolRepl == true)
        boolRepl = await connection.sIsMember(KEY1, "NotAMember")
        check(boolRepl == false)
        # SMISMEMBER key member [member ...] 
        boolReplArray = await connection.smIsMember(KEY1, 1, 2, "NotAMember")
        check(boolReplArray == @[true, true, false])
        expect RedisCommandError:
          boolReplArray = await connection.smIsMember(KEY1)
        # SCARD key 
        intRepl = await connection.sCard(KEY1)
        check(intRepl == 4)
        # SDIFF key [key ...]
        intRepl = await connection.del(KEY1)
        intRepl = await connection.sAdd(KEY1, "1", "2", "3", "4")
        intRepl = await connection.sAdd(KEY2, "1", "2", "4", "5")
        strReplArray = await connection.sDiff(KEY1, KEY2)
        check(strReplArray == @["3"])
        # SDIFFSTORE destination key [key ...]
        intRepl = await connection.sDiffStore(KEY3, KEY1, KEY2)
        check(intRepl == 1)
        strReplArray = await connection.sMembers(KEY3)
        check(strReplArray == @["3"])
        # SINTER key [key ...] 
        strReplArray = await connection.sInter(KEY1, KEY2)
        check(strReplArray == @["1", "2", "4"])
        # SINTERSTORE destination key [key ...] 
        intRepl = await connection.sInterStore(KEY3, KEY1, KEY2)
        check(intRepl == 3)
        strReplArray = await connection.sMembers(KEY3)
        check(strReplArray == @["1", "2", "4"])
        # SMOVE source destination member
        intRepl = await connection.del(KEY2, KEY3)
        boolRepl = await connection.sMove(KEY1, KEY2, "1")
        check(boolRepl == true)
        boolRepl = await connection.sMove(KEY1, KEY2, "NotAMember")
        check(boolRepl == false)
        strReplArray = await connection.sMembers(KEY2)
        check(strReplArray == @["1"])
        # SPOP key [count]
        optStrRepl = await connection.sPop(KEY1)
        check(optStrRepl in @["2".option, "3".option, "4".option])
        strReplArray = await connection.sPop(KEY1).count(2)
        check(strReplArray.len == 2)
        check(strReplArray == @["2", "4"] or strReplArray == @["3", "4"] or strReplArray == @["2", "3"])
        # SRANDMEMBER key [count] 
        intRepl = await connection.del(KEY1, KEY2)
        intRepl = await connection.sAdd(KEY1, "1", "2", "3")
        optStrRepl = await connection.sRandMember(KEY1)
        check(optStrRepl in @["1".option, "2".option, "3".option])
        strReplArray = await connection.sRandMember(KEY1).count(2)
        check(strReplArray == @["1", "2"] or
          strReplArray == @["1", "3"] or
          strReplArray == @["2", "3"] or
          strReplArray == @["3", "2"] or
          strReplArray == @["3", "1"] or
          strReplArray == @["2", "1"]
          )
        # SREM key member [member ...] 
        intRepl = await connection.sRem(KEY1, "1", "2", "NotAMember")
        check(intRepl == 2)
        strReplArray = await connection.sMembers(KEY1)
        check(strReplArray == @["3"])
        # SUNION key [key ...] 
        intRepl = await connection.del(KEY1, KEY2, KEY3)
        intRepl = await connection.sAdd(KEY1, "1", "2", "3", "4")
        intRepl = await connection.sAdd(KEY2, "1", "2", "5", "6")
        strReplArray = await connection.sUnion(KEY1, KEY2)
        check(strReplArray == @["1", "2", "3", "4", "5", "6"])
        # SUNIONSTORE destination key [key ...] 
        intRepl = await connection.sUnionStore(KEY3, KEY1, KEY2)
        check(intRepl == 6)
        strReplArray = await connection.sMembers(KEY3)
        check(strReplArray == @["1", "2", "3", "4", "5", "6"])
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testSets())

  test "Sorted sets commands":
    proc testSortedSets() {.async.} =
      var connection = newRedis("localhost", 6379, 0, poolsize=2, timeout=5000)
      try:
        var optStrRepl: Option[string]
        var intRepl: int64
        var optIntRepl: Option[int64]
        var floatRepl: float
        var optFloatRepl: Option[float]
        var strReplArray: seq[string]
        var zSetReplArray: seq[ZSetValue[float]]
        var optFloatReplArray: seq[Option[float]]
        await connection.connect()
        # ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2))
        check(intRepl == 2)
        intRepl = await connection.zAdd(KEY1, ("a", 2)).ch()
        check(intRepl == 1)
        intRepl = await connection.zAdd(KEY1, ("a", 7)).xx()
        check(intRepl == 0)
        intRepl = await connection.zAdd(KEY1, ("a", 2)).nx()
        check(intRepl == 0)
        intRepl = await connection.zAdd(KEY1, ("c", 3)).nx()
        check(intRepl == 1)
        floatRepl = await connection.zAddIncr(KEY1, ("a", 3))
        check(floatRepl == 10.0)
        intRepl = await connection.zAdd(KEY1, ("a", 2)).lt().ch()
        check(intRepl == 1)
        intRepl = await connection.zAdd(KEY1, ("b", 7)).lt().ch()
        check(intRepl == 0)
        intRepl = await connection.zAdd(KEY1, ("a", 10)).gt().ch()
        check(intRepl == 1)
        intRepl = await connection.zAdd(KEY1, ("b", 1)).gt().ch()
        check(intRepl == 0)
        # ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES] 
        strReplArray = await connection.zRange(KEY1, (0 .. -1))
        check(strReplArray == @["b", "c", "a"])
        strReplArray = await connection.zRange(KEY1, (0 .. -1)).rev()
        check(strReplArray == @["a", "c", "b"])
        zSetReplArray = await connection.zRange(KEY1, (0 .. -1)).rev().withScores()
        check(zSetReplArray == @[("a", 10.0), ("c", 3.0), ("b", 2.0)])
        zSetReplArray = await connection.zRange(KEY1, (0 .. 100)).byScore().limit(1, 1).withScores()
        check(zSetReplArray == @[("c", 3.0)])
        # ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] 
        intRepl = await connection.zRangeStore(KEY4, KEY1, (0 .. -1))
        check(intRepl == 3)
        strReplArray = await connection.zRange(KEY4, (0 .. -1))
        check(strReplArray == @["b", "c", "a"])
        intRepl = await connection.del(KEY4)
        # ZCARD key 
        intRepl = await connection.zCard(KEY1)
        check(intRepl == 3)
        # ZCOUNT key min max 
        intRepl = await connection.zCount(KEY1, (0 .. 3))
        check(intRepl == 2)
        # ZINCRBY key increment member 
        floatRepl = await connection.zIncrBy(KEY1, "a", 1.5)
        check(floatRepl == 11.5)
        # ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] 
        intRepl = await connection.del(KEY1, KEY2, KEY3, KEY4)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2))
        intRepl = await connection.zAdd(KEY2, ("a", 1), ("b", 2), ("c", 3))
        intRepl = await connection.zAdd(KEY3, ("a", 1), ("b", 2), ("d", 3))
        strReplArray = await connection.zInter(KEY1, KEY2, KEY3)
        check(strReplArray == @["a", "b"])
        strReplArray = await connection.zInter(KEY1, KEY2, KEY3).weights(1, 1, 2)
        check(strReplArray == @["a", "b"])
        zSetReplArray = await connection.zInter(KEY1, KEY2, KEY3).weights(1, 1, 2).withScores()
        check(zSetReplArray == @[("a", 4.0), ("b", 8.0)])
        # ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] 
        intRepl = await connection.del(KEY4)
        intRepl = await connection.zInterStore(KEY4, KEY1, KEY2, KEY3)
        check(intRepl == 2)
        zSetReplArray = await connection.zRange(KEY4, (0 .. -1)).withScores()
        check(zSetReplArray == @[("a", 3.0), ("b", 6.0)])
        intRepl = await connection.zInterStore(KEY4, KEY1, KEY2, KEY3).weights(1, 1, 2)
        check(intRepl == 2)
        zSetReplArray = await connection.zRange(KEY4, (0 .. -1)).withScores()
        check(zSetReplArray == @[("a", 4.0), ("b", 8.0)])
        # ZLEXCOUNT key min max 
        intRepl = await connection.del(KEY1, KEY2, KEY3, KEY4)
        intRepl = await connection.zAdd(KEY1, ("a", 0), ("b", 0), ("c", 0), ("d", 0), ("e", 0), ("f", 0), ("g", 0))
        intRepl = await connection.zLexCount(KEY1, ("-" .. "+"))
        check(intRepl == 7)
        intRepl = await connection.zLexCount(KEY1, ("[b" .. "[f"))
        check(intRepl == 5)
        # ZMSCORE key member [member ...] 
        intRepl = await connection.del(KEY1, KEY2, KEY3, KEY4)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2))
        optFloatReplArray = await connection.zmScore(KEY1, "a", "b", "noKey")
        check(optFloatReplArray == @[option(1.0), option(2.0), float.none])
        # ZPOPMAX key [count] 
        intRepl = await connection.del(KEY1, KEY2, KEY3, KEY4)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3), ("d", 4))
        zSetReplArray = await connection.zPopMax(KEY1)
        check(zSetReplArray == @[("d", 4.0)])
        zSetReplArray = await connection.zPopMax(KEY1, 2)
        check(zSetReplArray == @[("c", 3.0), ("b", 2.0)])
        # ZPOPMIN key [count] 
        intRepl = await connection.del(KEY1, KEY2, KEY3, KEY4)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3), ("d", 4))
        zSetReplArray = await connection.zPopMin(KEY1)
        check(zSetReplArray == @[("a", 1.0)])
        zSetReplArray = await connection.zPopMin(KEY1, 2)
        check(zSetReplArray == @[("b", 2.0), ("c", 3.0)])
        # ZRANDMEMBER key [count [WITHSCORES]] 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3), ("d", 4))
        optStrRepl = await connection.zRandMember(KEY1)
        check(optStrRepl.isSome)
        check(optStrRepl in @["a".option, "b".option, "c".option, "d".option])
        optStrRepl = await connection.zRandMember(KEY2)
        check(optStrRepl.isNone)
        strReplArray = await connection.zRandMember(KEY1, 3)
        check(strReplArray.len == 3)
        strReplArray = await connection.zRandMember(KEY1, 10)
        check(strReplArray.len == 4)
        strReplArray = await connection.zRandMember(KEY1, -3)
        check(strReplArray.len == 3)
        strReplArray = await connection.zRandMember(KEY1, -10)
        check(strReplArray.len == 10)
        zSetReplArray = await connection.zRandMember(KEY1, 2).withScores()
        check(zSetReplArray.len == 2)
        # ZRANGEBYLEX key min max [LIMIT offset count] 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 0), ("b", 0), ("c", 0), ("d", 0), ("e", 0), ("f", 0), ("g", 0))
        strReplArray = await connection.zRangeByLex(KEY1, ("-" .. "[c"))
        check(strReplArray == @["a", "b", "c"])
        strReplArray = await connection.zRangeByLex(KEY1, ("-" .. "(c"))
        check(strReplArray == @["a", "b"])
        strReplArray = await connection.zRangeByLex(KEY1, ("[aaa" .. "(g")).limit(1, 3)
        check(strReplArray == @["c", "d", "e"])
        # ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        strReplArray = await connection.zRangeByScore(KEY1, (-Inf .. +Inf))
        check(strReplArray == @["a", "b", "c"])
        strReplArray = await connection.zRangeByScore(KEY1, (1 .. 2))
        check(strReplArray == @["a", "b"])
        strReplArray = await connection.zRangeByScore(KEY1, ("(1" .. "2"))
        check(strReplArray == @["b"])
        zSetReplArray = await connection.zRangeByScore(KEY1, (1 .. 2)).withScores()
        check(zSetReplArray == @[("a", 1.0), ("b", 2.0)])
        # ZRANK key member 
        optIntRepl = await connection.zRank(KEY1, "b")
        check(optIntRepl == 1.int64.option)
        optIntRepl = await connection.zRank(KEY1, "z")
        check(optIntRepl.isNone)
        # ZREM key member [member ...] 
        intRepl = await connection.zRem(KEY1, "a", "b")
        check(intRepl == 2)
        strReplArray = await connection.zRange(KEY1, (0 .. -1))
        check(strReplArray == @["c"])
        # ZREMRANGEBYLEX key min max 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("aaaa", 0), ("b", 0), ("c", 0), ("d", 0), ("e", 0), ("foo", 0), ("zap", 0), ("zip", 0), ("ALPHA", 0), ("alpha", 0))
        intRepl = await connection.zRemRangeByLex(KEY1, ("[alpha" .. "[omega"))
        check(intRepl == 6)
        strReplArray = await connection.zRange(KEY1, (0 .. -1))
        check(strReplArray == @["ALPHA", "aaaa", "zap", "zip"])
        # ZREMRANGEBYRANK key start stop 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        intRepl = await connection.zRemRangeByRank(KEY1, (0 .. 1))
        check(intRepl == 2)
        zSetReplArray = await connection.zRange(KEY1, (0 .. -1)).withScores()
        check(zSetReplArray == @[("c", 3.0)])
        # ZREMRANGEBYSCORE key min max 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        intRepl = await connection.zRemRangeByScore(KEY1, ("-inf" .. "(2"))
        check(intRepl == 1)
        zSetReplArray = await connection.zRange(KEY1, (0 .. -1)).withScores()
        check(zSetReplArray == @[("b", 2.0), ("c", 3.0)])
        # ZREVRANGE key start stop [WITHSCORES] 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        strReplArray = await connection.zRevRange(KEY1, (0 .. -1))
        check(strReplArray == @["c", "b", "a"])
        zSetReplArray = await connection.zRevRange(KEY1, (0 .. -1)).withScores()
        check(zSetReplArray == @[("c", 3.0), ("b", 2.0), ("a", 1.0)])
        # ZREVRANGEBYLEX key max min [LIMIT offset count] 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 0), ("b", 0), ("c", 0), ("d", 0), ("e", 0), ("f", 0), ("g", 0))
        strReplArray = await connection.zRevRangeByLex(KEY1, ("[c" .. "-"))
        check(strReplArray == @["c", "b", "a"])
        strReplArray = await connection.zRevRangeByLex(KEY1, ("(g" .. "[aaa")).limit(1, 3)
        check(strReplArray == @["e", "d", "c"])
        # ZREVRANGEBYSCORE key max min [LIMIT offset count] [WITHSCORES]
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        strReplArray = await connection.zRevRangeByScore(KEY1, ("+inf" .. "-inf"))
        check(strReplArray == @["c", "b", "a"])
        strReplArray = await connection.zRevRangeByScore(KEY1, (2 .. 1))
        check(strReplArray == @["b", "a"])
        strReplArray = await connection.zRevRangeByScore(KEY1, ("+inf" .. "-inf")).limit(1, 1)
        check(strReplArray == @["b"])
        zSetReplArray = await connection.zRevRangeByScore(KEY1, (2 .. 1)).withScores()
        check(zSetReplArray == @[("b", 2.0), ("a", 1.0)])
        # ZREVRANK key member 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        optIntRepl = await connection.zRevRank(KEY1, "a")
        check(optIntRepl == 2'i64.option)
        optIntRepl = await connection.zRevRank(KEY1, "z")
        check(optIntRepl.isNone)
        # ZSCORE key member 
        intRepl = await connection.del(KEY1)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        optFloatRepl = await connection.zScore(KEY1, "a")
        check(optFloatRepl == 1.0.option)
        optFloatRepl = await connection.zScore(KEY1, "z")
        check(optFloatRepl.isNone)
        # ZDIFF numkeys key [key ...] [WITHSCORES] 
        intRepl = await connection.del(KEY1, KEY2)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        intRepl = await connection.zAdd(KEY2, ("a", 1), ("b", 2))
        strReplArray = await connection.zDiff(KEY1, KEY2)
        check(strReplArray == @["c"])
        zSetReplArray = await connection.zDiff(KEY1, KEY2).withScores()
        check(zSetReplArray == @[("c", 3.0)])
        # ZDIFFSTORE destination numkeys key [key ...] 
        intRepl = await connection.del(KEY1, KEY2, KEY3)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2), ("c", 3))
        intRepl = await connection.zAdd(KEY2, ("a", 1), ("b", 2))
        intRepl = await connection.zDiffStore(KEY3, KEY1, KEY2)
        check(intRepl == 1)
        strReplArray = await connection.zRange(KEY3, (0 .. -1))
        check(strReplArray == @["c"])
        # ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] 
        intRepl = await connection.del(KEY1, KEY2, KEY3)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2))
        intRepl = await connection.zAdd(KEY2, ("a", 1), ("b", 2), ("c", 3))
        strReplArray = await connection.zUnion(KEY1, KEY2)
        check(strReplArray == @["a", "c", "b"])
        zSetReplArray = await connection.zUnion(KEY1, KEY2).withScores()
        check(zSetReplArray == @[("a", 2.0), ("c", 3.0), ("b", 4.0)])
        zSetReplArray = await connection.zUnion(KEY1, KEY2).weights(1, 2).withScores()
        check(zSetReplArray == @[("a", 3.0), ("b", 6.0), ("c", 6.0)])
        # ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] 
        intRepl = await connection.del(KEY1, KEY2, KEY3)
        intRepl = await connection.zAdd(KEY1, ("a", 1), ("b", 2))
        intRepl = await connection.zAdd(KEY2, ("a", 1), ("b", 2), ("c", 3))
        intRepl = await connection.zUinonStore(KEY3, KEY1, KEY2)
        check(intRepl == 3)
        zSetReplArray = await connection.zRange(KEY3, (0 .. -1)).withScores()
        check(zSetReplArray == @[("a", 2.0), ("c", 3.0), ("b", 4.0)])
        intRepl = await connection.zUinonStore(KEY3, KEY1, KEY2).weights(2, 3)
        check(intRepl == 3)
        zSetReplArray = await connection.zRange(KEY3, (0 .. -1)).withScores()
        check(zSetReplArray == @[("a", 5.0), ("c", 9.0), ("b", 10.0)])
      except RedisConnectionError:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testSortedSets())
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
