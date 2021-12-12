import std/[asyncdispatch, options, times, strutils]
import ./cmd
import ../exceptions

#[
  Block of sorted sets commands
    BZPOPMAX
    BZPOPMIN
    *ZADD
    *ZCARD
    *ZCOUNT
    *ZDIFF
    *ZDIFFSTORE
    *ZINCRBY
    *ZINTER
    ZINTERCARD
    *ZINTERSTORE
    *ZLEXCOUNT
    *ZMSCORE
    *ZPOPMAX
    *ZPOPMIN
    *ZRANDMEMBER
    *ZRANGE
    *ZRANGEBYLEX
    *ZRANGEBYSCORE
    *ZRANGESTORE
    *ZRANK
    *ZREM
    *ZREMRANGEBYLEX
    *ZREMRANGEBYRANK
    *ZREMRANGEBYSCORE
    *ZREVRANGE
    *ZREVRANGEBYLEX
    *ZREVRANGEBYSCORE
    *ZREVRANK
    ZSCAN
    *ZSCORE
    *ZUNION
    *ZUNIONSTORE
]#

type
  ZSetValue*[T: SomeInteger | SomeFloat] = tuple[name: string, score: T]
  ZSetSlice* = Slice[string]
  
  RedisZAddRequest = ref object of RedisRequestT[int64]
    nx: bool
    xx: bool
    gt: bool
    lt: bool
    ch: bool

  RedisZAddIncrRequest = ref object of RedisRequestT[RedisStrFloat]
    gt: bool
    lt: bool
    ch: bool

  RedisAggregateType* = enum
    NONE = "NONE"
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
  
  RedisZIURequest = ref object of RedisArrayRequestT[string]
    keysN: int
    aggregate: RedisAggregateType
  
  RedisZIUWithScoresRequest = ref object of RedisArrayRequestT[ZSetValue[float]]
    keysN: int
    aggregate: RedisAggregateType
  
  RedisZIUWithStoreRequest = ref object of RedisRequestT[int64]
    keysN: int
    aggregate: RedisAggregateType
  
  RedisZRangeRequest = ref object of RedisArrayRequestT[string]
  RedisZRangeBLRequest = ref object of RedisArrayRequestT[string]
  RedisZRangeBSRequest = ref object of RedisArrayRequestT[string]
  RedisZEnableWithScoresRequest = ref object of RedisArrayRequestT[string]
  RedisZRangeStoreRequest = ref object of RedisRequestT[int64]
  RedisWithScoresRequest = ref object of RedisArrayRequestT[ZSetValue[float]]

proc checkInter(inter: string): bool {.inline.}

proc newRedisZAddRequest(redis: Redis): RedisZAddRequest =
  result = newRedisRequest[RedisZAddRequest](redis)
  result.nx = false
  result.xx = false
  result.gt = false
  result.lt = false
  result.ch = false

proc newRedisZAddIncrRequest(redis: Redis): RedisZAddIncrRequest =
  result = newRedisRequest[RedisZAddIncrRequest](redis)
  result.gt = false
  result.lt = false
  result.ch = false

proc newRedisZIURequest(redis: Redis): RedisZIURequest =
  result = newRedisRequest[RedisZIURequest](redis)
  result.aggregate = NONE
  result.keysN = 0

proc newRedisZIUWithScoresRequest[T: RedisZIURequest](req: T): RedisZIUWithScoresRequest =
  result = newFromRedisRequest[RedisZIUWithScoresRequest](req)
  result.aggregate = req.aggregate
  result.keysN = req.keysN

proc newRedisZIUWithStoreRequest(redis: Redis): RedisZIUWithStoreRequest =
  result = newRedisRequest[RedisZIUWithStoreRequest](redis)
  result.aggregate = NONE
  result.keysN = 0

proc toString(x: SomeInteger | SomeFloat): string = $x

proc fromRedisReq*(_: type[tuple[key: string, value: ZSetValue[float]]], req: RedisMessage): seq[tuple[key: string, value: ZSetValue[float]]]
proc fromRedisReq*(_: type[seq[ZSetValue[float]]], req: RedisMessage): seq[ZSetValue[float]]
proc withScores*[T: RedisZRangeRequest | RedisZRangeBLRequest | RedisZRangeBSRequest | RedisZEnableWithScoresRequest](req: T): RedisWithScoresRequest

# BZPOPMAX key [key ...] timeout
proc bzPopMax*(redis: Redis, timeout: Duration, key: string, keys=varargs[string]): RedisArrayRequestT[tuple[key: string, value: ZSetValue[float]]] =
  result = newRedisRequest[RedisArrayRequestT[tuple[key: string, value: ZSetValue[float]]]](redis)
  result.addCmd("BZPOPMAX", key)
  result.extend(keys)
  let floatTimeout = (timeout.inMilliseconds.toBiggestFloat())/1000.0
  result.add(floatTimeout)

# BZPOPMIN key [key ...] timeout 
proc bzPopMin*(redis: Redis, timeout: Duration, key: string, keys=varargs[string]): RedisArrayRequestT[tuple[key: string, value: ZSetValue[float]]] =
  result = newRedisRequest[RedisArrayRequestT[tuple[key: string, value: ZSetValue[float]]]](redis)
  result.addCmd("BZPOPMIN", key)
  result.extend(keys)
  let floatTimeout = (timeout.inMilliseconds.toBiggestFloat())/1000.0
  result.add(floatTimeout)

# ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
proc zAdd*[T: SomeInteger | SomeFloat](redis: Redis, key: string, member: ZSetValue[T], members: varargs[ZSetValue[T]]): RedisZAddRequest =
  result = newRedisZAddRequest(redis)
  result.addCmd("ZADD", key, $member.score, member.name)
  for memb in members:
    result.add($memb.score, memb.name)

proc zAddIncr*[T: SomeInteger | SomeFloat](redis: Redis, key: string, member: ZSetValue[T]): RedisZAddIncrRequest =
  result = newRedisZAddIncrRequest(redis)
  result.addCmd("ZADD", key, "INCR", $member.score, member.name)

proc lt*[T: RedisZAddRequest | RedisZAddIncrRequest](req: T): T =
  result = req
  if result.gt:
    raise newException(RedisCommandError, "Conflicting options for ZADD GT vs LT")
  if result.nx:
    raise newException(RedisCommandError, "Conflicting options for ZADD NX vs LT")
  if result.lt:
    raise newException(RedisCommandError, "LT already set")
  result.lt = true
  result.insert(2, "LT")

proc gt*[T: RedisZAddRequest | RedisZAddIncrRequest](req: T): T =
  result = req
  if result.lt:
    raise newException(RedisCommandError, "Conflicting options for ZADD LT vs GT")
  if result.nx:
    raise newException(RedisCommandError, "Conflicting options for ZADD NX vs GT")
  if result.gt:
    raise newException(RedisCommandError, "GT already set")
  result.gt = true
  result.insert(2, "GT")

proc ch*[T: RedisZAddRequest | RedisZAddIncrRequest](req: T): T =
  result = req
  if result.ch:
    raise newException(RedisCommandError, "CH already set")
  result.ch = true
  result.insert(2, "CH")

proc nx*(req: RedisZAddRequest): RedisZAddRequest =
  result = req
  if result.xx:
    raise newException(RedisCommandError, "Conflicting options for ZADD XX vs NX")
  if result.lt or result.gt:
    raise newException(RedisCommandError, "Conflicting options for ZADD LT,GT vs NX")
  if result.nx:
    raise newException(RedisCommandError, "NX already set")
  result.nx = true
  result.insert(2, "NX")

proc xx*(req: RedisZAddRequest): RedisZAddRequest =
  result = req
  if result.nx:
    raise newException(RedisCommandError, "Conflicting options for ZADD NX vs XX")
  if result.xx:
    raise newException(RedisCommandError, "XX already set")
  result.xx = true
  result.insert(2, "XX")

# ZCARD key 
proc zCard*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZCARD", key)

# ZCOUNT key min max 
proc zCount*[T: SomeInteger | SomeFloat](redis: Redis, key: string, ranges: Slice[T]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZCOUNT", key, $ranges.a, $ranges.b)

# ZDIFF numkeys key [key ...] [WITHSCORES] 
proc zDiff*(redis: Redis, key: string, keys: varargs[string]): RedisZEnableWithScoresRequest =
  result = newRedisRequest[RedisZEnableWithScoresRequest](redis)
  let keysAmount: int = 1+keys.len
  result.addCmd("ZDIFF", $keysAmount, key)
  result.extend(keys)

# ZDIFFSTORE destination numkeys key [key ...] 
proc zDiffStore*(redis: Redis, destKey: string, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  let keysAmount: int = 1+keys.len
  result.addCmd("ZDIFFSTORE", destKey, $keysAmount, key)
  result.extend(keys)

# ZINCRBY key increment member 
proc zIncrBy*[T: SomeInteger | SomeFloat](redis: Redis, key, member: string, increment: T): RedisRequestT[RedisStrFloat] =
  result = newRedisRequest[RedisRequestT[RedisStrFloat]](redis)
  result.addCmd("ZINCRBY", key, $increment, member)

# ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] 
proc zInter*(redis: Redis, key: string, keys: varargs[string]): RedisZIURequest =
  result = newRedisZIURequest(redis)
  result.keysN = keys.len+1
  result.addCmd("ZINTER", $result.keysN, key)
  result.extend(keys)

proc weights*[T: RedisZIURequest | RedisZIUWithScoresRequest | RedisZIUWithStoreRequest, V: SomeInteger | SomeFloat](req: T, weights: varargs[V]): T =
  result = req
  if result.keysN != weights.len:
    raise newException(RedisCommandError, "Keys and weights count must be equal")
  result.add("WEIGHTS")
  result.extend(weights, toString)

proc aggregate*[T: RedisZIURequest | RedisZIUWithScoresRequest | RedisZIUWithStoreRequest](req: T, aggType: RedisAggregateType): T =
  result = req
  if result.aggregate != NONE:
    raise newException(RedisCommandError, "AGGREGATE can be given only once")
  if aggType != NONE:
    result.add("AGGREGATE", $aggType)
    result.aggregate = aggType

proc withScores*[T: RedisZIURequest](req: T): RedisZIUWithScoresRequest =
  result = req.newRedisZIUWithScoresRequest()
  result.add("WITHSCORES")

# ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] 
proc zInterStore*(redis: Redis, destKey: string, key: string, keys: varargs[string]): RedisZIUWithStoreRequest =
  result = newRedisZIUWithStoreRequest(redis)
  result.keysN = keys.len+1
  result.addCmd("ZINTERSTORE", destKey, $result.keysN, key)
  result.extend(keys)

# ZINTERCARD numkeys key [key ...]
proc zInterCard*(redis: Redis, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  let keysAmount: int = 1+keys.len
  result.addCmd("ZINTERCARD", keysAmount, key)
  result.extend(keys)

# ZLEXCOUNT key min max 
proc zLexCount*(redis: Redis, key: string, minMax: ZSetSlice): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZLEXCOUNT command")
  result.addCmd("ZLEXCOUNT", key, minMax.a, minMax.b)

# ZMSCORE key member [member ...] 
proc zmScore*(redis: Redis, key: string, member: string, members: varargs[string]): RedisArrayRequestT[Option[RedisStrFloat]] =
  result = newRedisRequest[RedisArrayRequestT[Option[RedisStrFloat]]](redis)
  result.addCmd("ZMSCORE", key, member)
  result.extend(members)

# ZPOPMAX key [count] 
proc zPopMax*(redis: Redis, key: string): RedisArrayRequestT[ZSetValue[float]] =
  result = newRedisRequest[RedisArrayRequestT[ZSetValue[float]]](redis)
  result.addCmd("ZPOPMAX", key)

proc zPopMax*[T: SomeInteger](redis: Redis, key: string, count: T): RedisArrayRequestT[ZSetValue[float]] =
  result = redis.zPopMax(key)
  result.add($count)

# ZPOPMIN key [count] 
proc zPopMin*(redis: Redis, key: string): RedisArrayRequestT[ZSetValue[float]] =
  result = newRedisRequest[RedisArrayRequestT[ZSetValue[float]]](redis)
  result.addCmd("ZPOPMIN", key)

proc zPopMin*[T: SomeInteger](redis: Redis, key: string, count: T): RedisArrayRequestT[ZSetValue[float]] =
  result = redis.zPopMin(key)
  result.add($count)

# ZRANDMEMBER key [count [WITHSCORES]] 
proc zRandMember*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("ZRANDMEMBER", key)

proc zRandMember*[T: SomeInteger](redis: Redis, key: string, count: T): RedisZEnableWithScoresRequest =
  result = cast[RedisZEnableWithScoresRequest](zRandMember(redis, key))
  result.add($count)

# ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES] 
proc zRange*[T: SomeInteger | SomeFloat](redis: Redis, key: string, minMax: Slice[T]): RedisZRangeRequest =
  result = newRedisRequest[RedisZRangeRequest](redis)
  result.addCmd("ZRANGE", key, $minMax.a, $minMax.b)

proc zRange*(redis: Redis, key: string, minMax: ZSetSlice): RedisZRangeRequest =
  result = newRedisRequest[RedisZRangeRequest](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZRANGE command")
  result.addCmd("ZRANGE", key, minMax.a, minMax.b)

proc rev*[T: RedisZRangeRequest | RedisZRangeStoreRequest](req: T): T =
  result = req
  result.add("REV")

proc limit*[T: RedisZRangeBLRequest | RedisZRangeBSRequest | RedisZRangeStoreRequest](req: T, offset: int, count: int): T =
  result = req
  result.add("LIMIT", $offset, $count)

proc byScore*(req: RedisZRangeRequest): RedisZRangeBSRequest =
  result = cast[RedisZRangeBSRequest](req)
  result.add("BYSCORE")

proc byLex*(req: RedisZRangeRequest): RedisZRangeBLRequest =
  result = cast[RedisZRangeBLRequest](req)
  result.add("BYLEX")

# ZRANGEBYLEX key min max [LIMIT offset count] 
proc zRangeByLex*(redis: Redis, key: string, minMax: ZSetSlice): RedisZRangeBLRequest =
  result = newRedisRequest[RedisZRangeBLRequest](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZRANGEBYLEX command")
  result.addCmd("ZRANGEBYLEX", key, minMax.a, minMax.b)

# ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
proc zRangeByScore*[T: SomeInteger| SomeFloat](redis: Redis, key: string, minMax: Slice[T]): RedisZRangeBSRequest =
  result = newRedisRequest[RedisZRangeBSRequest](redis)
  result.addCmd("ZRANGEBYSCORE", key, $minMax.a, $minMax.b)

proc zRangeByScore*(redis: Redis, key: string, minMax: ZSetSlice): RedisZRangeBSRequest =
  result = newRedisRequest[RedisZRangeBSRequest](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZRANGEBYSCORE command")
  result.addCmd("ZRANGEBYSCORE", key, minMax.a, minMax.b)

# ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] 
proc zRangeStore*[T: SomeInteger | SomeFloat](redis: Redis, destKey: string, key: string, minMax: Slice[T]): RedisZRangeStoreRequest =
  result = newRedisRequest[RedisZRangeStoreRequest](redis)
  result.addCmd("ZRANGESTORE", destKey, key, $minMax.a, $minMax.b)

proc zRangeStore*(redis: Redis, destKey: string, key: string, minMax: ZSetSlice): RedisZRangeStoreRequest =
  result = newRedisRequest[RedisZRangeStoreRequest](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZRANGESTORE command")
  result.addCmd("ZRANGESTORE", key, minMax.a, minMax.b)

# ZRANK key member 
proc zRank*(redis: Redis, key, member: string): RedisRequestT[Option[int64]] =
  result = newRedisRequest[RedisRequestT[Option[int64]]](redis)
  result.addCmd("ZRANK", key, member)

# ZREM key member [member ...] 
proc zRem*(redis: Redis, key, member: string, members: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZREM", key, member)
  result.extend(members)

# ZREMRANGEBYLEX key min max 
proc zRemRangeByLex*(redis: Redis, key: string, minMax: ZSetSlice): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZREMRANGEBYLEX command")
  result.addCmd("ZREMRANGEBYLEX", key, minMax.a, minMax.b)

# ZREMRANGEBYRANK key start stop 
proc zRemRangeByRank*[T: SomeInteger](redis: Redis, key: string, minMax: Slice[T]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZREMRANGEBYRANK", key, $minMax.a, $minMax.b)

# ZREMRANGEBYSCORE key min max 
proc zRemRangeByScore*[T: SomeInteger | SomeFloat](redis: Redis, key: string, minMax: Slice[T]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZREMRANGEBYSCORE", key, $minMax.a, $minMax.b)

proc zRemRangeByScore*(redis: Redis, key: string, minMax: ZSetSlice): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZREMRANGEBYSCORE command")
  result.addCmd("ZREMRANGEBYSCORE", key, minMax.a, minMax.b)

# ZREVRANGE key start stop [WITHSCORES] 
proc zRevRange*[T: SomeInteger | SomeFloat](redis: Redis, key: string, minMax: Slice[T]): RedisZEnableWithScoresRequest =
  result = newRedisRequest[RedisZEnableWithScoresRequest](redis)
  result.addCmd("ZREVRANGE", key, $minMax.a, $minMax.b)

proc zRevRange*(redis: Redis, key: string, minMax: ZSetSlice): RedisZEnableWithScoresRequest =
  result = newRedisRequest[RedisZEnableWithScoresRequest](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZREVRANGE command")
  result.addCmd("ZREVRANGE", key, minMax.a, minMax.b)

# ZREVRANGEBYLEX key max min [LIMIT offset count] 
proc zRevRangeByLex*(redis: Redis, key: string, minMax: ZSetSlice): RedisZRangeBLRequest =
  result = newRedisRequest[RedisZRangeBLRequest](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZREVRANGEBYLEX command")
  result.addCmd("ZREVRANGEBYLEX", key,  minMax.a, minMax.b)

# ZREVRANGEBYSCORE key max min [LIMIT offset count] [WITHSCORES]
proc zRevRangeByScore*[T: SomeInteger | SomeFloat](redis: Redis, key: string, minMax: Slice[T]): RedisZRangeBSRequest =
  result = newRedisRequest[RedisZRangeBSRequest](redis)
  result.addCmd("ZREVRANGEBYSCORE", key, $minMax.a, $minMax.b)

proc zRevRangeByScore*(redis: Redis, key: string, minMax: ZSetSlice): RedisZRangeBSRequest =
  result = newRedisRequest[RedisZRangeBSRequest](redis)
  if minMax.a.checkInter() == false or minMax.b.checkInter() == false:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZREVRANGEBYSCORE command")
  result.addCmd("ZREVRANGEBYSCORE", key, minMax.a, minMax.b)

# ZREVRANK key member 
proc zRevRank*(redis: Redis, key, member: string): RedisRequestT[Option[int64]] =
  result = newRedisRequest[RedisRequestT[Option[int64]]](redis)
  result.addCmd("ZREVRANK", key, member)

# ZSCAN key cursor [MATCH pattern] [COUNT count] 
proc zScan*(redis: Redis, match: Option[string] = string.none, count: int = -1): RedisCursorRequestT[ZSetValue[float]] =
  result = newRedisCursor[RedisCursorRequestT[ZSetValue[float]]](redis)
  result.addCmd("ZSCAN", 0)
  if match.isSome:
    result.add("MATCH", match.get())
  if count > 0:
    result.add("COUNT", count)

proc next*(cursor: RedisCursorRequestT[ZSetValue[float]]): Future[tuple[stop: bool, res: ZSetValue[float]]] {.async.} =
  let res = await cast[RedisCursorRequest](cursor).next()
  if res[0]:
    result = (true, ("", 0.0))
  else:
    result = (false, (res[1].arr[0].str.get(), res[1].arr[1].str.get().parseFloat()))

# ZSCORE key member 
proc zScore*(redis: Redis, key, member: string): RedisRequestT[Option[RedisStrFloat]] =
  result = newRedisRequest[RedisRequestT[Option[RedisStrFloat]]](redis)
  result.addCmd("ZSCORE", key, member)

# ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] 
proc zUnion*(redis: Redis, key: string, keys: varargs[string]): RedisZIURequest =
  result = newRedisZIURequest(redis)
  result.keysN = keys.len+1
  result.addCmd("ZUNION", $result.keysN, key)
  result.extend(keys)

# ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] 
proc zUinonStore*(redis: Redis, destKey: string, key: string, keys: varargs[string]): RedisZIUWithStoreRequest =
  result = newRedisZIUWithStoreRequest(redis)
  result.keysN = keys.len+1
  result.addCmd("ZUNIONSTORE", destKey, $result.keysN, key)
  result.extend(keys)

#------- common

proc fromRedisReq*(_: type[tuple[key: string, value: ZSetValue[float]]], req: RedisMessage): seq[tuple[key: string, value: ZSetValue[float]]] =
  result = @[]
  if req.kind != REDIS_MESSAGE_NIL:
    for i in countup(0, req.arr.len-3, 3):
      let val = (req.arr[i].str.get(), (req.arr[i+1].str.get(), req.arr[i+2].str.get().parseFloat()))
      result.add(val)

proc fromRedisReq*(_: type[seq[ZSetValue[float]]], req: RedisMessage): seq[ZSetValue[float]] =
  result = @[]
  for i in countup(0, req.arr.len-2, 2):
    result.add((req.arr[i].str.get(), req.arr[i+1].str.get().parseFloat()))

proc withScores*[T: RedisZRangeRequest | RedisZRangeBLRequest | RedisZRangeBSRequest | RedisZEnableWithScoresRequest](req: T): RedisWithScoresRequest =
  result = cast[RedisWithScoresRequest](req)
  result.add("WITHSCORES")

#------- pvt

proc checkInter(inter: string): bool {.inline.} =
  case inter.len()
  of 0:
    result = false
  of 1:
    result = true
  else:
    if inter[0] notin ['(', ')', '[', ']', '-', '+']:
      result = false
    else:
      result = true
