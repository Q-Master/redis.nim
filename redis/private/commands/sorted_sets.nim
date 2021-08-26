import std/[asyncdispatch, options, times, strutils]
import ./cmd
import ../exceptions

#[
  Block of sorted sets commands
    BZPOPMAX
    BZPOPMIN
    ZADD
    ZCARD
    ZCOUNT
    ZDIFF
    ZDIFFSTORE
    ZINCRBY
    ZINTER
    ZINTERCARD
    ZINTERSTORE
    ZLEXCOUNT
    ZMSCORE
    ZPOPMAX
    ZPOPMIN
    ZRANDMEMBER
    ZRANGE
    ZRANGEBYLEX
    ZRANGEBYSCORE
    ZRANGESTORE
    ZRANK
    ZREM
    ZREMRANGEBYLEX
    ZREMRANGEBYRANK
    ZREMRANGEBYSCORE
    ZREVRANGE
    ZREVRANGEBYLEX
    ZREVRANGEBYSCORE
    ZREVRANK
    ZSCAN
    ZSCORE
    ZUNION
    ZUNIONSTORE
]#

type
  ZSetValue* = ref ZSetValueObj
  ZSetValueObj* = ref object of RootObj
    value*: string
    score*: float
  
  RedisZAddRequest* = ref object of RedisRequestT[int64]
    nx: bool
    xx: bool
    gt: bool
    lt: bool
  RedisZAddRequestIncr* = ref object of RedisZAddRequest

  RedisAggregateType* = enum
    NONE = "NONE"
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
  
  ZSetLimit* = ref object of RootObj
    offset*: int
    count*: int

proc newZSetValue*(value: string, score: SomeFloat): ZSetValue =
  result.new
  result.value = value
  result.score = score.float

proc newZSetLimit*(offset: int, count: int): ZSetLimit =
  result.new
  result.offset = offset
  result.count = count

proc newRedisZAddRequest(redis: Redis): RedisZAddRequest
proc addRev(req: RedisRequest, rev: bool) {.inline.}
proc addLimit(req: RedisRequest, limit: ZSetLimit) {.inline.}
proc addWeights[T: SomeInteger | SomeFloat](req: RedisRequest, weights: seq[T]) {.inline.}

# BZPOPMAX key [key ...] timeout
proc bzPopMax*(redis: Redis, timeout: Duration, key: string, keys=varargs[string]): RedisArrayRequest[tuple[key: string, value: ZSetValue]] =
  result = newRedisRequest[RedisArrayRequest[tuple[key: string, value: ZSetValue]]](redis)
  result.addCmd("BZPOPMAX", key)
  for k in keys:
    result.add(k)
  let floatTimeout = (timeout.inMilliseconds.toBiggestFloat())/1000.0
  result.add(floatTimeout)

proc fromRedisReq*(_: type[tuple[key: string, value: ZSetValue]], req: RedisMessage): seq[tuple[key: string, value: ZSetValue]] =
  result = @[]
  if req.kind != REDIS_MESSAGE_NIL:
    for i in countup(0, req.arr.len-3, 3):
      let val = (req.arr[i].str.get(), newZSetValue(req.arr[i+1].str.get(), req.arr[i+2].str.get().parseFloat()))
      result.add(val)

# BZPOPMIN key [key ...] timeout 
proc bzPopMin*(redis: Redis, timeout: Duration, key: string, keys=varargs[string]): RedisArrayRequest[tuple[key: string, value: ZSetValue]] =
  result = newRedisRequest[RedisArrayRequest[tuple[key: string, value: ZSetValue]]](redis)
  result.addCmd("BZPOPMIN", key)
  for k in keys:
    result.add(k)
  let floatTimeout = (timeout.inMilliseconds.toBiggestFloat())/1000.0
  result.add(floatTimeout)

# ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
proc zAdd*(redis: Redis, key: string, member: ZSetValue, members: varargs[ZSetValue]): RedisZAddRequest =
  result = newRedisZAddRequest(redis)
  result.addCmd("ZADD", key, " ", " ", " ", member.value, member.value)
  for memb in members:
    result.add(memb.score, memb.value)

proc zAddIncr*(redis: Redis, key: string, member: ZSetValue): RedisZAddRequestIncr =
  result = cast[RedisZAddRequestIncr](newRedisZAddRequest(redis))
  result.addCmd("ZADD", key, " ", " ", " ", "INCR", member.value, member.value)

proc lt*[T: RedisZAddRequest](req: T, t: Duration): T =
  result = req
  if result.gt:
    raise newException(RedisConnectionError, "Conflicting options for ZADD GT vs LT")
  result.lt = true
  result.req.arr[3] = "LT".encodeRedis()

proc gt*[T: RedisZAddRequest](req: T, t: Duration): T =
  result = req
  if result.lt:
    raise newException(RedisConnectionError, "Conflicting options for ZADD LT vs GT")
  result.gt = true
  result.req.arr[3] = "GT".encodeRedis()

proc nx*[T: RedisZAddRequest](req: T): T =
  result = req
  if result.xx:
    raise newException(RedisConnectionError, "Conflicting options for ZADD XX vs NX")
  result.nx = true
  result.req.arr[2] = "NX".encodeRedis()

proc xx*[T: RedisZAddRequest](req: T): T =
  result = req
  if result.nx:
    raise newException(RedisConnectionError, "Conflicting options for ZADD NX vs XX")
  result.xx = true
  result.req.arr[2] = "XX".encodeRedis()

proc ch*[T: RedisZAddRequest](req: T): T =
  result = req
  result.req.arr[4] = "CH".encodeRedis()

proc execute*(req: RedisZAddRequest): Future[int64] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  result = res.integer

proc execute*(req: RedisZAddRequestIncr): Future[Option[float]] {.async.} =
  let res = await cast[RedisRequest](req).execute()
  if res.str.isSome:
    result = res.str.get().parseFloat().option
  else:
    result = float.none

# ZCARD key 
proc zCard*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZCARD", key)

# ZCOUNT key min max 
proc zCount*(redis: Redis, ranges: Slice[float]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZCOUNT", ranges.a, ranges.b)

# ZDIFF numkeys key [key ...] [WITHSCORES] 
proc zDiff*(redis: Redis, key: string, keys: varargs[string]): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  let keysAmount: int = 1+keys.len
  result.addCmd("ZDIFF", keysAmount, key)
  for k in keys:
    result.add(k)

proc zDiffWithScores*(redis: Redis, key: string, keys: varargs[string]): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](redis.zDiff(key, keys))
  result.add("WITHSCORES")

proc fromRedisReq*(_: type[RedisArrayRequest[ZSetValue]], req: RedisMessage): seq[ZSetValue] =
  result = @[]
  for i in countup(0, req.arr.len-2, 2):
    result.add(newZSetValue(req.arr[i].str.get(), req.arr[i+1].str.get().parseFloat()))

# ZDIFFSTORE destination numkeys key [key ...] 
proc zDiffStore*(redis: Redis, destKey: string, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  let keysAmount: int = 1+keys.len
  result.addCmd("ZDIFFSTORE", destKey, keysAmount, key)
  for k in keys:
    result.add(k)

# ZINCRBY key increment member 
proc zIncrBy*[T: SomeInteger | SomeFloat](redis: Redis, key, member: string, increment: T): RedisRequestT[RedisStrFloat] =
  result = newRedisRequest[RedisRequestT[RedisStrFloat]](redis)
  result.addCmd("ZINCRBY", key, increment.float, member)

# ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] 
proc zInter*[T: SomeInteger | SomeFloat](redis: Redis, keys: seq[string], weights: seq[T] = @[], aggregate: RedisAggregateType = NONE): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  let keysAmount: int = keys.len
  if keysAmount == 0:
    raise newException(RedisCommandError, "At least one key must present in ZINTER command")
  if weights.len > 0 and weights.len != keysAmount:
    raise newException(RedisCommandError, "Keys and weights count must be equal in ZINTER command")
  result.addCmd("ZINTER", keysAmount)
  for key in keys:
    result.add(key)
  result.addWeights(weights)
  if aggregate != NONE:
    result.add("AGGREGATE", $aggregate)

proc zInterWithScores*[T: SomeInteger | SomeFloat](redis: Redis, keys: seq[string], weights: seq[T] = @[], aggregate: RedisAggregateType = NONE): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zInter(redis, keys, weights, aggregate))
  result.add("WITHSCORES")

# ZINTERCARD numkeys key [key ...]
proc zInterCard*(redis: Redis, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  let keysAmount: int = 1+keys.len
  result.addCmd("ZINTERCARD", keysAmount, key)
  for k in keys:
    result.add(k)

# ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] 
proc zInterStore*[T: SomeInteger | SomeFloat](redis: Redis, destKey: string, keys: seq[string], weights: seq[T] = @[], aggregate: RedisAggregateType = NONE): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  let keysAmount: int = keys.len
  if keysAmount == 0:
    raise newException(RedisCommandError, "At least one key must present in ZINTERSTORE command")
  if weights.len > 0 and weights.len != keysAmount:
    raise newException(RedisCommandError, "Keys and weights count must be equal in ZINTERSTORE command")
  result.addCmd("ZINTERSTORE", keysAmount)
  for key in keys:
    result.add(key)
  result.addWeights(weights)
  if aggregate != NONE:
    result.add("AGGREGATE", $aggregate)

# ZLEXCOUNT key min max 
proc checkInter(inter: string): bool =
  case inter.len()
  of 0:
    result = false
  of 1:
    result = true
  of 2:
    if inter[0] notin ['(', ']']:
      result = false
    else:
      result = true
  else:
    result = false

proc zLexCount*(redis: Redis, key, min, max: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  if min.checkInter() and max.checkInter():
    result.addCmd("ZLEXCOUNT", key, min, max)
  else:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZLEXCOUNT command")

# ZMSCORE key member [member ...] 
proc zmScore*(redis: Redis, key: string, member: string, members: varargs[string]): RedisArrayRequest[Option[RedisStrFloat]] =
  result = newRedisRequest[RedisArrayRequest[Option[RedisStrFloat]]](redis)
  result.addCmd("ZMSCORE", key, member)
  for m in members:
    result.add(m)

# ZPOPMAX key [count] 
proc zPopMax*[T: SomeInteger](redis: Redis, key: string, count: Option[T] = T.none): RedisArrayRequest[ZSetValue] =
  result = newRedisRequest[RedisArrayRequest[ZSetValue]](redis)
  result.addCmd("ZPOPMAX", key)
  if count.isSome:
    result.add(count.get().int64)

# ZPOPMIN key [count] 
proc zPopMin*[T: SomeInteger](redis: Redis, key: string, count: Option[T] = T.none): RedisArrayRequest[ZSetValue] =
  result = newRedisRequest[RedisArrayRequest[ZSetValue]](redis)
  result.addCmd("ZPOPMIN", key)
  if count.isSome:
    result.add(count.get().int64)

# ZRANDMEMBER key [count [WITHSCORES]] 
proc zRandMember*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("ZRANDMEMBER", key)

proc zRandMember*[T: SomeInteger](redis: Redis, key: string, count: T): RedisArrayRequest[string] =
  result = cast[RedisArrayRequest[string]](zRandMember(redis, key))
  result.add(count.int64)

proc zRandMemberWithScores*[T: SomeInteger](redis: Redis, key: string, count: T): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zRandMember(redis, key, count))
  result.add("WITHSCORES")

# ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES] 
proc zRange*(redis: Redis, key: string, minMax: Slice[int], rev: bool = false, limit: ZSetLimit = nil): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  result.addCmd("ZRANGE", key, minMax.a, minMax.b)
  result.addRev(rev)
  result.addLimit(limit)

proc zRangeWithScores*(redis: Redis, key: string, minMax: Slice[int], rev: bool = false, limit: ZSetLimit = nil): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zRange(redis, key, minMax, rev, limit))
  result.add("WITHSCORES")

proc zRange*(redis: Redis, key: string, minMax: Slice[float], rev: bool = false, limit: ZSetLimit = nil): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  result.addCmd("ZRANGE", key, minMax.a, minMax.b, "BYSCORE")
  result.addRev(rev)
  result.addLimit(limit)

proc zRangeWithScores*(redis: Redis, key: string, minMax: Slice[float], rev: bool = false, limit: ZSetLimit = nil): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zRange(redis, key, minMax, rev, limit))
  result.add("WITHSCORES")

proc zRange*(redis: Redis, key: string, min, max: string, rev: bool = false, limit: ZSetLimit = nil): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  if min.checkInter() and max.checkInter():
    result.addCmd("ZRANGE", key, min, max, "BYLEX")
  else:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZRANGE command")
  result.addRev(rev)
  result.addLimit(limit)

proc zRangeWithScores*(redis: Redis, key: string, min, max: string, rev: bool = false, limit: ZSetLimit = nil): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zRange(redis, key, min, max, rev, limit))
  result.add("WITHSCORES")

# ZRANGEBYLEX key min max [LIMIT offset count] 
proc zRangeByLex*(redis: Redis, key: string, min, max: string, rev: bool = false, limit: ZSetLimit = nil): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  if min.checkInter() and max.checkInter():
    result.addCmd("ZRANGEBYLEX", key, min, max)
  else:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZRANGEBYLEX command")
  result.addLimit(limit)

# ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
proc zRangeByScore*(redis: Redis, key: string, minMax: Slice[float], limit: ZSetLimit = nil): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  result.addCmd("ZRANGEBYSCORE", key, minMax.a, minMax.b)
  result.addLimit(limit)

proc zRangeByScoreWithScores*(redis: Redis, key: string, minMax: Slice[float], limit: ZSetLimit = nil): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zRangeByScore(redis, key, minMax, limit))
  result.add("WITHSCORES")

# ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] 
proc zRangeStore*(redis: Redis, destKey: string, key: string, minMax: Slice[int], rev: bool = false, limit: ZSetLimit = nil): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZRANGESTORE", destKey, key, minMax.a, minMax.b)
  result.addRev(rev)
  result.addLimit(limit)

proc zRangeStore*(redis: Redis, destKey: string, key: string, minMax: Slice[float], rev: bool = false, limit: ZSetLimit = nil): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZRANGESTORE", destKey, key, minMax.a, minMax.b, "BYSCORE")
  result.addRev(rev)
  result.addLimit(limit)

proc zRangeStore*(redis: Redis, destKey: string, key: string, min, max: string, rev: bool = false, limit: ZSetLimit = nil): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  if min.checkInter() and max.checkInter():
    result.addCmd("ZRANGESTORE", destKey, key, min, max, "BYLEX")
  else:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZRANGESTORE command")
  result.addRev(rev)
  result.addLimit(limit)

# ZRANK key member 
proc zRank*(redis: Redis, key, member: string): RedisRequestT[Option[int64]] =
  result = newRedisRequest[RedisRequestT[Option[int64]]](redis)
  result.addCmd("ZRANK", key, member)

# ZREM key member [member ...] 
proc zRem*(redis: Redis, key, member: string, members: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZREM", key, member)
  for m in members:
    result.add(m)

# ZREMRANGEBYLEX key min max 
proc zRemRangeByLex*(redis: Redis, key, min, max: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  if checkInter(min) and checkInter(max):
    result.addCmd("ZREMRANGEBYLEX", key, min, max)
  else:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZREMRANGEBYLEX command")

# ZREMRANGEBYRANK key start stop 
proc zRemRangeByRank*(redis: Redis, key: string, minMax: Slice[int]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZREMRANGEBYRANK", key, minMax.a, minMax.b)

# ZREMRANGEBYSCORE key min max 
proc zRemRangeByScore*(redis: Redis, key: string, minMax: Slice[float]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("ZREMRANGEBYSCORE", key, minMax.a, minMax.b)

# ZREVRANGE key start stop [WITHSCORES] 
proc zRevRange*(redis: Redis, key: string, minMax: Slice[int]): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  result.addCmd("ZREVRANGE", key, minMax.a, minMax.b)

proc zRevRangeWithScores*(redis: Redis, key: string, minMax: Slice[int]): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zRevRange(redis, key, minMax))
  result.add("WITHSCORES")

# ZREVRANGEBYLEX key max min [LIMIT offset count] 
proc zRevRangeByLex*(redis: Redis, key, min, max: string, limit: ZSetLimit = nil): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  if checkInter(min) and checkInter(max):
    result.addCmd("ZREVRANGEBYLEX", key, min, max)
  else:
    raise newException(RedisCommandError, "min and max values must conform the documentation of ZREVRANGEBYLEX command")
  result.addLimit(limit)

# ZREVRANGEBYSCORE key max min [LIMIT offset count] [WITHSCORES]
proc zRevRangeByScore*(redis: Redis, key: string, minMax: Slice[float], limit: ZSetLimit = nil): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  result.addCmd("ZREVRANGEBYSCORE", key, minMax.a, minMax.b)
  result.addLimit(limit)

proc zRevRangeByScoreWithScores*(redis: Redis, key: string, minMax: Slice[float], limit: ZSetLimit = nil): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zRevRangeByScore(redis, key, minMax, limit))
  result.add("WITHSCORES")

# ZREVRANK key member 
proc zRevRank*(redis: Redis, key, member: string): RedisRequestT[Option[int64]] =
  result = newRedisRequest[RedisRequestT[Option[int64]]](redis)
  result.addCmd("ZREVRANK", key, member)

# ZSCAN key cursor [MATCH pattern] [COUNT count] 
proc zScan*(redis: Redis, match: Option[string] = string.none, count: int = -1): RedisCursorRequestT[ZSetValue] =
  result = newRedisCursor[RedisCursorRequestT[ZSetValue]](redis)
  result.addCmd("ZSCAN", 0)
  if match.isSome:
    result.add("MATCH", match.get())
  if count > 0:
    result.add("COUNT", count)

proc next*(cursor: RedisCursorRequestT[ZSetValue]): Future[tuple[stop: bool, res: ZSetValue]] {.async.} =
  let res = await cast[RedisCursorRequest](cursor).next()
  if res[0]:
    result = (true, nil)
  else:
    result = (false, newZSetValue(res[1].arr[0].str.get(), res[1].arr[1].str.get().parseFloat()))

# ZSCORE key member 
proc zScore*(redis: Redis, key, member: string): RedisRequestT[RedisStrFloat] =
  result = newRedisRequest[RedisRequestT[RedisStrFloat]](redis)
  result.addCmd("ZSCORE", key, member)

# ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES] 
proc zUnion*[T: SomeInteger | SomeFloat](redis: Redis, keys: seq[string], weights: seq[T] = @[], aggregate: RedisAggregateType = NONE): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  let keysAmount: int = keys.len
  if keysAmount == 0:
    raise newException(RedisCommandError, "At least one key must present in ZUNION command")
  if weights.len > 0 and weights.len != keysAmount:
    raise newException(RedisCommandError, "Keys and weights count must be equal in ZUNION command")
  result.addCmd("ZUNION", keysAmount)
  for key in keys:
    result.add(key)
  result.addWeights(weights)
  if aggregate != NONE:
    result.add("AGGREGATE", $aggregate)

proc zUnionWithScores*[T: SomeInteger | SomeFloat](redis: Redis, keys: seq[string], weights: seq[T] = @[], aggregate: RedisAggregateType = NONE): RedisArrayRequest[ZSetValue] =
  result = cast[RedisArrayRequest[ZSetValue]](zInter(redis, keys, weights, aggregate))
  result.add("WITHSCORES")

# ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] 
proc zUnionStore*[T: SomeInteger | SomeFloat](redis: Redis, destKey: string, keys: seq[string], weights: seq[T] = @[], aggregate: RedisAggregateType = NONE): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  let keysAmount: int = keys.len
  if keysAmount == 0:
    raise newException(RedisCommandError, "At least one key must present in ZUNIONSTORE command")
  if weights.len > 0 and weights.len != keysAmount:
    raise newException(RedisCommandError, "Keys and weights count must be equal in ZUNIONSTORE command")
  result.addCmd("ZUNIONSTORE", keysAmount)
  for key in keys:
    result.add(key)
  result.addWeights(weights)
  if aggregate != NONE:
    result.add("AGGREGATE", $aggregate)

#------- pvt

proc newRedisZAddRequest(redis: Redis): RedisZAddRequest =
  result = newRedisRequest[RedisZAddRequest](redis)
  result.nx = false
  result.xx = false
  result.gt = false
  result.lt = false

proc addRev(req: RedisRequest, rev: bool) {.inline.} =
  if rev:
    req.add("REV")

proc addLimit(req: RedisRequest, limit: ZSetLimit) {.inline.} =
  if not limit.isNil:
    req.add("LIMIT", limit.offset, limit.count)

proc addWeights[T: SomeInteger | SomeFloat](req: RedisRequest, weights: seq[T]) {.inline.} =
  if weights.len > 0:
    req.add("WEIGHTS")
    for weight in weights:
      req.add(weight.float)
