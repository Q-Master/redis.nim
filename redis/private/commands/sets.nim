import std/[options]
import ./cmd
import ../exceptions

#[
  Block of sets commands
    *SADD
    *SCARD
    *SDIFF
    *SDIFFSTORE
    *SINTER
    SINTERCARD
    *SINTERSTORE
    *SISMEMBER
    *SMEMBERS
    *SMISMEMBER
    *SMOVE
    *SPOP
    *SRANDMEMBER
    *SREM
    SSCAN
    *SUNION
    *SUNIONSTORE
]#

type
  RedisSRandPopRequestT = ref object of RedisRequestT[Option[string]]

# SADD key member [member ...]
proc sAdd*(redis: Redis, key: string, members: varargs[string, `$`]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SADD", key)
  if members.len == 0:
    raise newException(RedisCommandError, "SADD must have at least one member to check")
  result.extend(members)

# SCARD key 
proc sCard*(redis: Redis, key: string): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SCARD", key)

# SDIFF key [key ...]
proc sDiff*(redis: Redis, key: string, keys: varargs[string]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("SDIFF", key)
  result.extend(keys)

# SDIFFSTORE destination key [key ...]
proc sDiffStore*(redis: Redis, destKey, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SDIFFSTORE", destKey, key)
  result.extend(keys)

# SINTER key [key ...] 
proc sInter*(redis: Redis, key: string, keys: varargs[string]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("SINTER", key)
  result.extend(keys)

# SINTERCARD key [key ...]
proc sInterCard*(redis: Redis, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SINTERCARD", key)
  result.extend(keys)

# SINTERSTORE destination key [key ...] 
proc sInterStore*(redis: Redis, destKey, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SINTERSTORE", destKey, key)
  result.extend(keys)

# SISMEMBER key member
proc sIsMember*(redis: Redis, key, member: string): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("SISMEMBER", key, member)

# SMEMBERS key 
proc sMembers*(redis: Redis, key: string): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("SMEMBERS", key)

# SMISMEMBER key member [member ...] 
proc smIsMember*(redis: Redis, key: string, members: varargs[string, `$`]): RedisArrayRequestT[RedisIntBool] =
  result = newRedisRequest[RedisArrayRequestT[RedisIntBool]](redis)
  result.addCmd("SMISMEMBER", key)
  if members.len == 0:
    raise newException(RedisCommandError, "SMISMEMBER must have at least one member to check")
  result.extend(members)

# SMOVE source destination member
proc sMove*(redis: Redis, srcKey, destKey, member: string): RedisRequestT[RedisIntBool] =
  result = newRedisRequest[RedisRequestT[RedisIntBool]](redis)
  result.addCmd("SMOVE", srcKey, destKey, member)

# SPOP key [count]
proc sPop*(redis: Redis, key: string): RedisSRandPopRequestT =
  result = newRedisRequest[RedisSRandPopRequestT](redis)
  result.addCmd("SPOP", key)

# SRANDMEMBER key [count] 
proc sRandMember*(redis: Redis, key: string): RedisSRandPopRequestT =
  result = newRedisRequest[RedisSRandPopRequestT](redis)
  result.addCmd("SRANDMEMBER", key)

proc count*(req: RedisSRandPopRequestT, count: SomeInteger): RedisArrayRequestT[string] =
  result = cast[RedisArrayRequestT[string]](req)
  result.add($count)

# SREM key member [member ...] 
proc sRem*(redis: Redis, key: string, members: varargs[string, `$`]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SREM", key)
  if members.len == 0:
    raise newException(RedisCommandError, "SREM must have at least one member to check")
  result.extend(members)

# SSCAN key cursor [MATCH pattern] [COUNT count] 
proc sScan*(redis: Redis, match: Option[string] = string.none, count: int = -1): RedisCursorRequestT[string] =
  result = newRedisCursor[RedisCursorRequestT[string]](redis)
  result.addCmd("SSCAN", 0)
  if match.isSome:
    result.add("MATCH", match.get())
  if count > 0:
    result.add("COUNT", count)

# SUNION key [key ...] 
proc sUnion*(redis: Redis, key: string, keys: varargs[string]): RedisArrayRequestT[string] =
  result = newRedisRequest[RedisArrayRequestT[string]](redis)
  result.addCmd("SUNION", key)
  result.extend(keys)

# SUNIONSTORE destination key [key ...] 
proc sUnionStore*(redis: Redis, destKey, key: string, keys: varargs[string]): RedisRequestT[int64] =
  result = newRedisRequest[RedisRequestT[int64]](redis)
  result.addCmd("SUNIONSTORE", destKey, key)
  result.extend(keys)
