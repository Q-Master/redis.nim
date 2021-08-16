import std/[options, tables]
import ./cmd

#[
  Block of hashes commands 
    HDEL
    HEXISTS
    HGET
    HGETALL
    HINCRBY
    HINCRBYFLOAT
    HKEYS
    HLEN
    HMGET
    HMSET
    HRANDFIELD
    HSCAN
    HSET
    HSETNX
    HSTRLEN
    HVALS
]#

# HDEL key field [field ...] 
proc hDel*(redis: Redis, key, field: string, fields: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("HDEL", key, field)
  if fields.len > 0:
    result.add(data = fields)

# HEXISTS key field 
proc hExists*(redis: Redis, key, field: string): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("HEXISTS", key, field)

# HGET key field 
proc hGet*(redis: Redis, key, field: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("HGET", key, field)

# HGETALL key 
proc hGetAll*(redis: Redis, key: string): RedisArrayRequest[Table[string, string]] =
  result = newRedisRequest[RedisArrayRequest[Table[string, string]]](redis)
  result.addCmd("HGETALL", key)

proc fromRedisReq*(req: RedisArrayRequest[Table[string, string]]): Table[string, string] =
  result = initTable[string, string]()
  for i in countup(0, req.req.arr.len-1, 2):
    result[req.req.arr[i].str.get()] = req.req.arr[i+1].str.get()

# HINCRBY key field increment 
proc hIncrBy*(redis: Redis, key, field: string, increment: SomeInteger): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("HINCRBY", key, field, increment.int64)

# HINCRBYFLOAT key field increment 
proc hIncrBy*(redis: Redis, key, field: string, increment: SomeFloat): RedisRequestT[float] =
  result = newRedisRequest[RedisRequestT[float]](redis)
  result.addCmd("HINCRBYFLOAT", key, field, increment.float)

# HKEYS key 
proc hKeys*(redis: Redis, key: string): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  result.addCmd("HKEYS", key)

# HLEN key 
proc hLen*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("HLEN", key)

# HMGET key field [field ...] 
proc hmGet*(redis: Redis, key, field: string, fields: varargs[RedisMessage, encodeRedis]): RedisArrayRequest[Option[string]] =
  result = newRedisRequest[RedisArrayRequest[Option[string]]](redis)
  result.addCmd("HMGET", key, field)
  if fields.len > 0:
    result.add(data = fields)

# HMSET key field value [field value ...] 
proc hmSet*[T](redis: Redis, key: string, fieldValue: tuple[a: string, b: T], fieldValues: varargs[tuple[a: string, b: T]]): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("HMSET", key, fieldValue.a, fieldValue.b)
  for kv in fieldValues:
    result.add(kv.a, kv.b)

# HRANDFIELD key [count [WITHVALUES]] 
proc hRandField*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result = newRedisRequest[RedisRequestT[Option[string]]](redis)
  result.addCmd("HRANDFIELD", key)

proc hRandField*(redis: Redis, key: string, count: SomeInteger): RedisArrayRequest[Option[string]] =
  result = newRedisRequest[RedisArrayRequest[Option[string]]](redis)
  result.addCmd("HRANDFIELD", key, count.int64)

proc hRandFieldWithValues*(redis: Redis, key: string, count: SomeInteger): RedisArrayRequest[Table[string, string]] =
  result = newRedisRequest[RedisArrayRequest[Table[string, string]]](redis)
  result.addCmd("HRANDFIELD", key, count.int64, "WITHVALUES")

# HSET key field value [field value ...] 
proc hSet*[T](redis: Redis, key: string, fieldValue: tuple[a: string, b: T], fieldValues: varargs[tuple[a: string, b: T]]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("HSET", key, fieldValue.a, fieldValue.b)
  for kv in fieldValues:
    result.add(kv.a, kv.b)

# HSETNX key field value 
proc hSetNX*[T](redis: Redis, key, field: string, value: T): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("HSETNX", key, field, value)

# HSTRLEN key field 
proc hStrLen*(redis: Redis, key, field: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("HSTRLEN", key, field)

# HVALS key 
proc hVals*(redis: Redis, key: string): RedisArrayRequest[string] =
  result = newRedisRequest[RedisArrayRequest[string]](redis)
  result.addCmd("HVALS", key)