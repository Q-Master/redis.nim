import std/strutils

type
  RedisError* = object of Defect
  RedisTypeError* = object of RedisError
  RedisProtocolError* = object of RedisError
  RedisAuthError* = object of RedisError
  RedisConnectionError* = object of RedisError
  RedisExecAbortError* = object of RedisError
  RedisBusyLoadingError* = object of RedisError
  RedisNoScriptError* = object of RedisError
  RedisReadOnlyError* = object of RedisError
  RedisNoPermissionError* = object of RedisError
  RedisWrongNumberOfArgsError* = object of RedisError
  RedisModuleError* = object of RedisError
  RedisCommandError* = object of RedisError

const MAX_NUMBER_OF_CLIENTS_REACHED = "max number of clients reached"
const CLIENT_SENT_AUTH_NO_PASSWORD = "Client sent AUTH, but no password is set"
const INVALID_PASSWORD = "invalid password"
const WRONG_NUMBER_OF_ARGUMETNS_FOR_AUTH_LOW = "wrong number of arguments for 'auth' command"
const WRONG_NUMBER_OF_ARGUMETNS_FOR_AUTH_HIGH = "wrong number of arguments for 'AUTH' command"
const MODULE_LOAD_ERROR = "Error loading the extension. Please check the server logs."
const NO_SUCH_MODULE_ERROR = "Error unloading module: no such module with that name"
const MODULE_UNLOAD_NOT_POSSIBLE_ERROR = "Error unloading module: operation not possible."
const MODULE_EXPORTS_DATA_TYPES_ERROR = "Error unloading module: the module exports one or more module-side data types, can't unload"

proc raiseException*(error: string) =
  let splt = error.split(" ", 1)
  let errCode = splt[0]
  let errorString = splt[1] 
  if errCode == "ERR":
    if errorString == MAX_NUMBER_OF_CLIENTS_REACHED:
      raise newException(RedisConnectionError, errorString)
    elif errorString == CLIENT_SENT_AUTH_NO_PASSWORD:
      raise newException(RedisAuthError, errorString)
    elif errorString == INVALID_PASSWORD:
      raise newException(RedisAuthError, errorString)
    elif errorString == WRONG_NUMBER_OF_ARGUMETNS_FOR_AUTH_LOW or errorString == WRONG_NUMBER_OF_ARGUMETNS_FOR_AUTH_HIGH:
      raise newException(RedisWrongNumberOfArgsError, errorString)
    elif errorString == MODULE_LOAD_ERROR:
      raise newException(RedisModuleError, errorString)
    elif errorString == NO_SUCH_MODULE_ERROR:
      raise newException(RedisModuleError, errorString)
    elif errorString == MODULE_UNLOAD_NOT_POSSIBLE_ERROR:
      raise newException(RedisModuleError, errorString)
    elif errorString == MODULE_EXPORTS_DATA_TYPES_ERROR:
      raise newException(RedisModuleError, errorString)
  elif errCode == "EXECABORT":
    raise newException(RedisExecAbortError, errorString)
  elif errCode == "LOADING":
    raise newException(RedisBusyLoadingError, errorString)
  elif errCode == "NOSCRIPT":
    raise newException(RedisNoScriptError, errorString)
  elif errCode == "READONLY":
    raise newException(RedisReadOnlyError, errorString)
  elif errCode == "NOAUTH":
    raise newException(RedisAuthError, errorString)
  elif errCode == "NOPERM":
    raise newException(RedisNoPermissionError, errorString)
  else:
    raise newException(RedisError, errorString)