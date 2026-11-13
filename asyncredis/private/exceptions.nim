import std/strutils


type
  RedisError* = object of CatchableError
  RedisTypeError* = object of RedisError
  RedisProtocolError* = object of RedisError        ## RESP-protocol errors
  RedisAuthError* = object of RedisError
  RedisConnectionError* = object of RedisError
  RedisExecAbortError* = object of RedisError
  RedisBusyLoadingError* = object of RedisError
  RedisNoScriptError* = object of RedisError
  RedisReadOnlyError* = object of RedisError
  RedisNoPermissionError* = object of RedisError
  RedisWrongNumberOfArgsError* = object of RedisError
  RedisModuleError* = object of RedisError
  RedisCommandError* = object of RedisError         ## Command erroros (ERR ...)
  RedisCommandNotYetImplementedError* = object of RedisError
  RedisKeyDoesntExist* = object of RedisError
  RedisKeyDoesntExpire* = object of RedisError
  RedisCursorExhausted* = object of RedisError


const
  MAX_NUMBER_OF_CLIENTS_REACHED = "max number of clients reached"
  CLIENT_SENT_AUTH_NO_PASSWORD = "Client sent AUTH, but no password is set"
  INVALID_PASSWORD = "invalid password"
  WRONG_NUMBER_OF_ARGUMENTS_FOR_AUTH_LOW = "wrong number of arguments for 'auth' command"
  WRONG_NUMBER_OF_ARGUMENTS_FOR_AUTH_HIGH = "wrong number of arguments for 'AUTH' command"


proc raiseException*(error: string) =
  let sp = error.find(' ')
  let code = if sp < 0: error else: error[0 ..< sp]
  let msg  = if sp < 0: ""    else: error[sp + 1 .. ^1]
  case code
  of "WRONGPASS", "NOAUTH":
    raise newException(RedisAuthError, msg)
  of "NOPERM":
    raise newException(RedisNoPermissionError, msg)
  of "LOADING":
    raise newException(RedisBusyLoadingError, msg)
  of "NOSCRIPT":
    raise newException(RedisNoScriptError, msg)
  of "READONLY":
    raise newException(RedisReadOnlyError, msg)
  of "EXECABORT":
    raise newException(RedisExecAbortError, msg)
  of "ERR":
    if msg == MAX_NUMBER_OF_CLIENTS_REACHED:
      raise newException(RedisConnectionError, msg)
    elif msg == CLIENT_SENT_AUTH_NO_PASSWORD or msg == INVALID_PASSWORD:
      raise newException(RedisAuthError, msg)
    elif msg == WRONG_NUMBER_OF_ARGUMENTS_FOR_AUTH_LOW or
        msg == WRONG_NUMBER_OF_ARGUMENTS_FOR_AUTH_HIGH:
      raise newException(RedisWrongNumberOfArgsError, msg)
    elif msg.startsWith("Error loading the extension") or
        msg.startsWith("Error unloading module"):
      raise newException(RedisModuleError, msg)
    else:
      raise newException(RedisCommandError, msg)
  else:
    raise newException(RedisError, error)
