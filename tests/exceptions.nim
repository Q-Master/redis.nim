import std/[unittest]
import asyncredis/private/exceptions

suite "Redis connection":
  setup:
    discard

  test "raiseException mapping":
    expect RedisAuthError:
      raiseException("WRONGPASS invalid username-password pair")
    expect RedisAuthError:
      raiseException("NOAUTH Authentication required")
    expect RedisNoPermissionError:
      raiseException("NOPERM user has no permission")
    expect RedisBusyLoadingError:
      raiseException("LOADING Redis is loading the dataset")
    expect RedisNoScriptError:
      raiseException("NOSCRIPT No matching script")
    expect RedisReadOnlyError:
      raiseException("READONLY You can't write against a read only replica")
    expect RedisExecAbortError:
      raiseException("EXECABORT Transaction discarded")
    expect RedisConnectionError:
      raiseException("ERR max number of clients reached")
    expect RedisWrongNumberOfArgsError:
      raiseException("ERR wrong number of arguments for 'auth' command")
    expect RedisModuleError:
      raiseException("ERR Error unloading module: no such module with that name")
    expect RedisCommandError:
      raiseException("ERR DEBUG command not allowed")
    expect RedisCommandError:
      raiseException("ERR wrong number of arguments for 'sadd' command")
    expect RedisError:
      raiseException("SOMECODE")          # без пробела — не падает
    expect RedisError:
      raiseException("MOVED 3999 127.0.0.1:6381")
