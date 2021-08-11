import std/[asyncdispatch, strutils, tables, times, macros, options]
import ./cmd
import ../exceptions

#[
  Block of sets commands
    SADD
    SCARD
    SDIFF
    SDIFFSTORE
    SINTER
    SINTERCARD
    SINTERSTORE
    SISMEMBER
    SMEMBERS
    SMISMEMBER
    SMOVE
    SPOP
    SRANDMEMBER
    SREM
    SSCAN
    SUNION
    SUNIONSTORE
]#

#  SADD key member [member ...]
proc sAdd*(redis: Redis, key: string, members: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SADD", key)
  result.add(data = members)

# SCARD key 
proc sCard*(redis: Redis, key: string): RedisRequestT[int64] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SCARD", key)

# SDIFF key [key ...]
proc sDiff*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisArrayRequest[string] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SDIFF", key)
  result.add(data = keys)

# SDIFFSTORE destination key [key ...]
proc sDiffStore*(redis: Redis, destKey, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SDIFFSTORE", destKey, key)
  result.add(data = keys)

# SINTER key [key ...] 
proc sInter*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisArrayRequest[string] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SINTER", key)
  result.add(data = keys)

# SINTERCARD key [key ...]
proc sInterCard*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SINTERCARD", key)
  result.add(data = keys)

# SINTERSTORE destination key [key ...] 
proc sInterStore*(redis: Redis, destKey, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SINTERSTORE", destKey, key)
  result.add(data = keys)

# SISMEMBER key member
proc sIsMember*(redis: Redis, key, member: string): RedisRequestT[RedisIntBool] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SISMEMBER", key, member)

# SMEMBERS key 
proc sMembers*(redis: Redis, key: string): RedisArrayRequest[string] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SMEMBERS", key)

# SMISMEMBER key member [member ...] 
proc smIsMember*(redis: Redis, key, member: string, members: varargs[RedisMessage, encodeRedis]): RedisArrayRequest[RedisIntBool] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SMISMEMBER", key, member)
  result.add(data = members)

# SMOVE source destination member
proc sMove*(redis: Redis, srcKey, destKey, member: string): RedisRequestT[RedisIntBool] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SMOVE", srcKey, destKey, member)

# SPOP key [count]
proc sPop*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SPOP", key)

proc sPop*(redis: Redis, key: string, count: SomeInteger): RedisArrayRequest[string] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SPOP", key, count.int64)

# SRANDMEMBER key [count] 
proc sRandMember*(redis: Redis, key: string): RedisRequestT[Option[string]] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SRANDMEMBER", key)

proc sRandMember*(redis: Redis, key: string, count: SomeInteger): RedisArrayRequest[string] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SRANDMEMBER", key, count.int64)

# SREM key member [member ...] 
proc sRem*(redis: Redis, key, member: string, members: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SREM", key, member)
  result.add(data = members)

# SUNION key [key ...] 
proc sUnion*(redis: Redis, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisArrayRequest[string] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SUNION", key)
  result.add(data = keys)

# SUNIONSTORE destination key [key ...] 
proc sUnionStore*(redis: Redis, destKey, key: string, keys: varargs[RedisMessage, encodeRedis]): RedisRequestT[int64] =
  result.new
  result.initRedisRequest(redis)
  result.addCmd("SUNIONSTORE", destKey, key)
  result.add(data = keys)
