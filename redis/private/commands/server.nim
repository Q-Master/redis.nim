import std/[asyncdispatch, times, options]
import ./cmd

#[
  Block of server commands
    ACL CAT
    ACL DELUSER
    ACL GENPASS
    ACL GETUSER
    ACL HELP
    ACL LIST
    ACL LOAD
    ACL LOG
    ACL SAVE
    ACL SETUSER
    ACL USERS
    ACL WHOAMI
    BGREWRITEAOF
    BGSAVE
    COMMAND
    COMMAND COUNT
    COMMAND GETKEYS
    COMMAND INFO
    CONFIG GET
    CONFIG RESETSTAT
    CONFIG REWRITE
    CONFIG SET
    DBSIZE
    DEBUG OBJECT
    DEBUG SEGFAULT
    FAILOVER
    FLUSHALL
    FLUSHDB
    INFO
    LASTSAVE
    LATENCY DOCTOR
    LATENCY GRAPH
    LATENCY HELP
    LATENCY HISTORY
    LATENCY LATEST
    LATENCY RESET
    LOLWUT
    MEMORY DOCTOR
    MEMORY HELP
    MEMORY MALLOC-STATS
    MEMORY PURGE
    MEMORY STATS
    MEMORY USAGE
    MODULE LIST
    MODULE LOAD
    MODULE UNLOAD
    MONITOR
    PSYNC
    REPLICAOF
    ROLE
    SAVE
    SHUTDOWN
    SLAVEOF
    SLOWLOG
    SWAPDB
    SYNC
    TIME
]#

type
  RedisFlushOption* = enum
    REDIS_SET_NONE
    REDIS_SET_ASYNC = "ASYNC"
    REDIS_SET_SYNC = "SYNC"

# FLUSHALL [ASYNC|SYNC] 
proc flushAll*(redis: Redis, flushType: RedisFlushOption = REDIS_SET_NONE): RedisRequestT[RedisStrBool] =
  result = newRedisRequest[RedisRequestT[RedisStrBool]](redis)
  result.addCmd("FLUSHALL")
  if flushType != REDIS_SET_NONE:
    result.add($flushType)
