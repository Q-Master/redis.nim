import std/[sha1]
import ./cmd

#[
  Block of scripting commands 
    EVAL
    EVALSHA
    EVALSHA_RO
    EVAL_RO
    SCRIPT DEBUG
    SCRIPT EXISTS
    SCRIPT FLUSH
    SCRIPT KILL
    SCRIPT LOAD
]#

# EVAL script numkeys [key [key ...]] [arg [arg ...]] 
proc eval*[T](redis: Redis, script: string, ro: bool = false, keyArgs: varargs[tuple[a: string, b: T]]): RedisRequest =
  result = newRedisRequest[RedisRequest](redis)
  let keysAmount = keyArgs.len()
  result.addCmd((if ro: "EVAL_RO" else: "EVAL"), script, keysAmount)
  if keysAmount > 0:
    for ka in keyArgs:
      result.add(ka.a)
    for ka in keyArgs:
      result.add(ka.b)

# EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]] 
proc eval*[T](redis: Redis, sha1: SecureHash, ro: bool = false, keyArgs: varargs[tuple[a: string, b: T]]): RedisRequest =
  result = newRedisRequest[RedisRequest](redis)
  let keysAmount = keyArgs.len()
  result.addCmd((if ro: "EVALSHA_RO" else: "EVALSHA"), sha1, keysAmount)
  if keysAmount > 0:
    for ka in keyArgs:
      result.add(ka.a)
    for ka in keyArgs:
      result.add(ka.b)

# SCRIPT EXISTS sha1 [sha1 ...] 
proc scriptExists*(redis: Redis, sha1: SecureHash, sha1s: varargs[SecureHash]): RedisArrayRequestT[RedisIntBool] =
  result = newRedisRequest[RedisArrayRequestT[RedisIntBool]](redis)
  result.addCmd("SCRIPT EXISTS", sha1)
  result.extend(sha1s)

# SCRIPT FLUSH [ASYNC|SYNC] 
proc scriptFlush*(redis: Redis, async: bool = false): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("SCRIPT FLUSH", (if async: "ASYNC" else: "SYNC"))

# SCRIPT LOAD script 
proc scriptLoad*(redis: Redis, script: string): RedisRequestT[SecureHash] =
  result = newRedisRequest[RedisRequestT[SecureHash]](redis)
  result.addCmd("SCRIPT LOAD", script)
