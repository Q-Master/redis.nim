import std/asyncdispatch
import std/[strutils, sets, tables]
import ./connection
import ./exceptions

type
  RedisMessageTypes* = enum
    REDIS_REPLY_ERROR,
    REDIS_REPLY_STATUS,
    REDIS_REPLY_INTEGER,
    REDIS_REPLY_DOUBLE,
    REDIS_REPLY_NIL,
    REDIS_REPLY_STRING,
    REDIS_REPLY_ARRAY,
    REDIS_REPLY_MAP,
    REDIS_REPLY_SET,
    REDIS_REPLY_BOOL,
    REDIS_REPLY_VERB,
    REDIS_REPLY_PUSH,
    REDIS_REPLY_BIGNUM,
    REDIS_REPLY_ATTRS


  RedisMessage* = ref RedisMessageObj
  RedisMessageObj* = object of RootObj
    case messageType: RedisMessageTypes
    of REDIS_REPLY_ERROR:
      error: string
    of REDIS_REPLY_INTEGER:
      integer: int64
    of REDIS_REPLY_DOUBLE:
      double: float
    of REDIS_REPLY_NIL:
      discard
    of REDIS_REPLY_STATUS, REDIS_REPLY_STRING, REDIS_REPLY_VERB:
      str: ref string
    of REDIS_REPLY_ARRAY, REDIS_REPLY_PUSH:
      arr: seq[RedisMessage]
    of REDIS_REPLY_MAP:
      map: Table[string, RedisMessage]
    of REDIS_REPLY_SET:
      hashSet: HashSet[RedisMessage]
    of REDIS_REPLY_BOOL:
      b: bool
    of REDIS_REPLY_BIGNUM:
      bignum: string
    of REDIS_REPLY_ATTRS:
      discard

proc processLineItem(resTyp: RedisMessageTypes, item: string, key: bool): RedisMessage
proc processBulkItem(redis: Redis, resTyp: RedisMessageTypes, item: string, key: bool): Future[RedisMessage] {.async.}
proc processAggregateItem(redis: Redis, resTyp: RedisMessageTypes, item: string): Future[RedisMessage] {.async.}
proc parseResponse*(redis: Redis, key: bool = false): Future[RedisMessage] {.async.} =
  while true:
    # We are skipping the ATTRIBUTE type of redis reply always.
    let response = await redis.readLine()
    var tp: RedisMessageTypes
    case response[0]
    of '-':
      tp = REDIS_REPLY_ERROR
    of '+':
      tp = REDIS_REPLY_STATUS
    of ':':
      tp = REDIS_REPLY_INTEGER
    of ',':
      tp = REDIS_REPLY_DOUBLE
    of '_':
      tp = REDIS_REPLY_NIL
    of '$':
      tp = REDIS_REPLY_STRING
    of '*':
      tp = REDIS_REPLY_ARRAY
    of '%':
      tp = REDIS_REPLY_MAP
    of '~':
      tp = REDIS_REPLY_SET
    of '#':
      tp = REDIS_REPLY_BOOL
    of '=':
      tp = REDIS_REPLY_VERB
    of '>':
      tp = REDIS_REPLY_PUSH
    of '(':
      tp = REDIS_REPLY_BIGNUM
    of '|':
      tp = REDIS_REPLY_ATTRS
    else:
      raise newException(RedisTypeError, "Wrong type of a redis message")

    if key:
      # We should only parse base types as keys not translating them from text.
      if tp notin {REDIS_REPLY_STATUS, REDIS_REPLY_BIGNUM, REDIS_REPLY_BOOL, REDIS_REPLY_DOUBLE, REDIS_REPLY_INTEGER, REDIS_REPLY_STRING, REDIS_REPLY_VERB}:
        raise newException(ValueError, "Unsupported keys for map and attribute types")
    
    case tp
    of REDIS_REPLY_ERROR, REDIS_REPLY_STATUS, REDIS_REPLY_INTEGER, REDIS_REPLY_DOUBLE, REDIS_REPLY_NIL, REDIS_REPLY_BOOL, REDIS_REPLY_BIGNUM:
      result = processLineItem(tp, response[1 .. ^1], key);
    of REDIS_REPLY_STRING, REDIS_REPLY_VERB:
      result = await processBulkItem(redis, tp, response[1 .. ^1], key);
    of REDIS_REPLY_ARRAY, REDIS_REPLY_MAP, REDIS_REPLY_SET, REDIS_REPLY_PUSH:
      result = await processAggregateItem(redis, tp, response[1 .. ^1]);
    of REDIS_REPLY_ATTRS:
      let _ = await processAggregateItem(redis, tp, response[1 .. ^1]);
    if tp notin {REDIS_REPLY_ATTRS, REDIS_REPLY_PUSH}:
      break

template toBiggestInt(item: string): BiggestInt =
  let tmp = parseBiggestInt(item)
  if tmp < low(int64) or tmp > high(int64):
    raise newException(RedisProtocolError, "Bad INTEGER value")
  tmp

proc processLineItem(resTyp: RedisMessageTypes, item: string, key: bool): RedisMessage =
  if key:
    result = RedisMessage(messageType: REDIS_REPLY_STRING)
    result.str[] = item
  else:
    result = RedisMessage(messageType: resTyp)
    case resTyp
    of REDIS_REPLY_INTEGER:
      try:
        result.integer = toBiggestInt(item)
      except ValueError:
        raise newException(RedisProtocolError, "Bad INTEGER value")
    of REDIS_REPLY_DOUBLE:
      result.double = parseFloat(item)
    of REDIS_REPLY_BOOL:
      if item.len != 1 and item notin ["t", "T", "f", "F"]:
        raise newException(RedisProtocolError, "Bad BOOL value")
      result.b = item in ["T", "t"]
    of REDIS_REPLY_BIGNUM:
      for i in 0 ..< item.len:
        if i == 0 and item[i] == '-':
          continue
        if item[i] notin {'0' .. '9'}:
          raise newException(RedisProtocolError, "Bad BIGNUM value")
      result.bignum = item
    of REDIS_REPLY_NIL:
      if item.len != 0:
        raise newException(RedisProtocolError, "Bad NIL value")
    of REDIS_REPLY_ERROR:
      result.error = item
      raiseException(item)
    of REDIS_REPLY_STATUS:
      result.str[] = item
    else:
      raise newException(RedisTypeError, "Wrong type for line item")

proc processBulkItem(redis: Redis, resTyp: RedisMessageTypes, item: string, key: bool): Future[RedisMessage] {.async.} =
  result = RedisMessage(messageType: resTyp)
  let stringSize = toBiggestInt(item)
  if stringSize == -1:
    if key:
      raise newException(RedisProtocolError, "Map key can't be nil")
    result.str = nil
  else:
    let str = await redis.readRawString((stringSize+2).int)
    result.str[] = str[0 .. ^2]
    if result.str[].len != stringSize:
      raise newException(RedisProtocolError, "String/verb length is wrong")
    if result.messageType == REDIS_REPLY_VERB and str.len < 6 and str[3] != ':':
      raise newException(RedisProtocolError, "Verbatim string 4 bytes of content type are missing or incorrectly encoded. length is wrong")

proc processAggregateItem(redis: Redis, resTyp: RedisMessageTypes, item: string): Future[RedisMessage] {.async.} =
  result = RedisMessage(messageType: resTyp)
  let arraySize = toBiggestInt(item)
  case resTyp
  of REDIS_REPLY_ARRAY, REDIS_REPLY_PUSH:
    result.arr = @[]
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        result.arr.add(await parseResponse(redis))
  of REDIS_REPLY_MAP:
    result.map = initTable[string, RedisMessage]()
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        let key = await redis.parseResponse(key = true)
        let value = await redis.parseResponse()
        result.map[key.str[]] = value
  of REDIS_REPLY_SET:
    result.hashSet = initHashSet[RedisMessage]()
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        result.hashSet.incl(await redis.parseResponse())
  of REDIS_REPLY_ATTRS:
    # It is really a map like MAP reply type, but we'll ignore it by now.
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        let key = await redis.parseResponse(key = true)
        let value = await redis.parseResponse()
  else:
    raise newException(RedisTypeError, "Wrong type for aggregate item")
