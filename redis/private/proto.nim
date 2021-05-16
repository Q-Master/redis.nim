import std/asyncdispatch
import std/[strutils, sets, tables]
import ./connection
import ./exceptions

type
  RedisMessageTypes* = enum
    REDIS_MESSAGE_ERROR,
    REDIS_MESSAGE_SIMPLESTRING,
    REDIS_MESSAGE_INTEGER,
    REDIS_MESSAGE_DOUBLE,
    REDIS_MESSAGE_NIL,
    REDIS_MESSAGE_STRING,
    REDIS_MESSAGE_ARRAY,
    REDIS_MESSAGE_MAP,
    REDIS_MESSAGE_SET,
    REDIS_MESSAGE_BOOL,
    REDIS_MESSAGE_VERB,
    REDIS_MESSAGE_PUSH,
    REDIS_MESSAGE_BIGNUM,
    REDIS_MESSAGE_ATTRS


  RedisMessage* = ref RedisMessageObj
  RedisMessageObj* = object of RootObj
    case messageType: RedisMessageTypes
    of REDIS_MESSAGE_ERROR:
      error: string
    of REDIS_MESSAGE_INTEGER:
      integer: int64
    of REDIS_MESSAGE_DOUBLE:
      double: float
    of REDIS_MESSAGE_NIL:
      discard
    of REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_STRING, REDIS_MESSAGE_VERB:
      str: ref string
    of REDIS_MESSAGE_ARRAY, REDIS_MESSAGE_PUSH:
      arr: seq[RedisMessage]
    of REDIS_MESSAGE_MAP:
      map: Table[string, RedisMessage]
    of REDIS_MESSAGE_SET:
      hashSet: HashSet[RedisMessage]
    of REDIS_MESSAGE_BOOL:
      b: bool
    of REDIS_MESSAGE_BIGNUM:
      bignum: string
    of REDIS_MESSAGE_ATTRS:
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
      tp = REDIS_MESSAGE_ERROR
    of '+':
      tp = REDIS_MESSAGE_SIMPLESTRING
    of ':':
      tp = REDIS_MESSAGE_INTEGER
    of ',':
      tp = REDIS_MESSAGE_DOUBLE
    of '_':
      tp = REDIS_MESSAGE_NIL
    of '$':
      tp = REDIS_MESSAGE_STRING
    of '*':
      tp = REDIS_MESSAGE_ARRAY
    of '%':
      tp = REDIS_MESSAGE_MAP
    of '~':
      tp = REDIS_MESSAGE_SET
    of '#':
      tp = REDIS_MESSAGE_BOOL
    of '=':
      tp = REDIS_MESSAGE_VERB
    of '>':
      tp = REDIS_MESSAGE_PUSH
    of '(':
      tp = REDIS_MESSAGE_BIGNUM
    of '|':
      tp = REDIS_MESSAGE_ATTRS
    else:
      raise newException(RedisTypeError, "Wrong type of a redis message")

    if key:
      # We should only parse base types as keys not translating them from text.
      if tp notin {REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_BIGNUM, REDIS_MESSAGE_BOOL, REDIS_MESSAGE_DOUBLE, REDIS_MESSAGE_INTEGER, REDIS_MESSAGE_STRING, REDIS_MESSAGE_VERB}:
        raise newException(ValueError, "Unsupported keys for map and attribute types")
    
    case tp
    of REDIS_MESSAGE_ERROR, REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_INTEGER, REDIS_MESSAGE_DOUBLE, REDIS_MESSAGE_NIL, REDIS_MESSAGE_BOOL, REDIS_MESSAGE_BIGNUM:
      result = processLineItem(tp, response[1 .. ^1], key);
    of REDIS_MESSAGE_STRING, REDIS_MESSAGE_VERB:
      result = await processBulkItem(redis, tp, response[1 .. ^1], key);
    of REDIS_MESSAGE_ARRAY, REDIS_MESSAGE_MAP, REDIS_MESSAGE_SET, REDIS_MESSAGE_PUSH:
      result = await processAggregateItem(redis, tp, response[1 .. ^1]);
    of REDIS_MESSAGE_ATTRS:
      let _ = await processAggregateItem(redis, tp, response[1 .. ^1]);
    if tp notin {REDIS_MESSAGE_ATTRS, REDIS_MESSAGE_PUSH}:
      break


template encodeString(str: string, prefix = "$") =
  result.add(prefix & $str.len)
  result.add(str)

proc prepareRequest*(request: RedisMessage): seq[string] =
  result = @[]
  case request.messageType
  of REDIS_MESSAGE_INTEGER:
    result.add(":" & $request.integer)
  of REDIS_MESSAGE_DOUBLE:
    result.add("," & $request.double)
  of REDIS_MESSAGE_SIMPLESTRING:
    result.add("+" & request.str[])
  of REDIS_MESSAGE_STRING:
    request.str[].encodeString()
  of REDIS_MESSAGE_ARRAY:
    result.add("*" & $request.arr.len)
    for elem in request.arr:
      result.add(elem.prepareRequest())
  of REDIS_MESSAGE_MAP:
    result.add("%")
    result.add($request.map.len)
    for k,v in request.map:
      k.encodeString()
      result.add(v.prepareRequest())
  of REDIS_MESSAGE_SET:
    result.add("~")
    result.add($request.hashSet.len)
    for elem in request.hashSet:
      result.add(elem.prepareRequest())
  of REDIS_MESSAGE_BOOL:
    result.add("#")
    result.add(if request.b: "t" else: "f")
  of REDIS_MESSAGE_VERB:
    request.str[].encodeString("=")
  of REDIS_MESSAGE_BIGNUM:
    result.add("(" & request.bignum)
  else:
    raise newException(RedisTypeError, "Unsupported type for outgoing requests")

proc encode[T: SomeSignedInt | SomeUnsignedInt](x: T): RedisMessage
proc encode[T: float | float32 | float64](x: T): RedisMessage
proc encode(x: string): RedisMessage
proc encode(x: bool): RedisMessage
proc encode[T](x: openArray[T]): RedisMessage
proc encodeCommand*(cmd: string, args: varargs[RedisMessage, encode]): RedisMessage =
  result = RedisMessage(messageType: REDIS_MESSAGE_ARRAY)
  for c in cmd.splitWhitespace():
    result.arr.add(c.encode())
  for arg in args:
    result.arr.add(arg)

#[
proc `$`(msg: RedisMessage): string =
  case msg.messageType
  of REDIS_MESSAGE_INTEGER:
    result = result & $msg.integer
  of REDIS_MESSAGE_DOUBLE:
    result = result & $msg.double
  of REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_STRING:
    result = result & msg.str[]
  of REDIS_MESSAGE_ARRAY:
    result = result & "["
    var first = true
    for elem in msg.arr:
      if not first:
        result = result & ", "
      else:
        first = false
      result = result & $elem
    result = result & "]"
  of REDIS_MESSAGE_MAP:
    result = result & "{\n"
    var first = true
    result.add("%")
    result.add($request.map.len)
    for k,v in request.map:
      k.encodeString()
      result.add(v.prepareRequest())
  of REDIS_MESSAGE_SET:
    result.add("~")
    result.add($request.hashSet.len)
    for elem in request.hashSet:
      result.add(elem.prepareRequest())
  of REDIS_MESSAGE_BOOL:
    result.add("#")
    result.add(if request.b: "t" else: "f")
  of REDIS_MESSAGE_VERB:
    request.str[].encodeString("=")
  of REDIS_MESSAGE_BIGNUM:
    result.add("(" & request.bignum)
  else:
    raise newException(RedisTypeError, "Unsupported type for outgoing requests")
]#
#------- pvt

template toBiggestInt(item: string): BiggestInt =
  let tmp = parseBiggestInt(item)
  if tmp < low(int64) or tmp > high(int64):
    raise newException(RedisProtocolError, "Bad INTEGER value")
  tmp

proc processLineItem(resTyp: RedisMessageTypes, item: string, key: bool): RedisMessage =
  if key:
    result = RedisMessage(messageType: REDIS_MESSAGE_STRING)
    result.str[] = item
  else:
    result = RedisMessage(messageType: resTyp)
    case resTyp
    of REDIS_MESSAGE_INTEGER:
      try:
        result.integer = toBiggestInt(item)
      except ValueError:
        raise newException(RedisProtocolError, "Bad INTEGER value")
    of REDIS_MESSAGE_DOUBLE:
      result.double = parseFloat(item)
    of REDIS_MESSAGE_BOOL:
      if item.len != 1 and item notin ["t", "T", "f", "F"]:
        raise newException(RedisProtocolError, "Bad BOOL value")
      result.b = item in ["T", "t"]
    of REDIS_MESSAGE_BIGNUM:
      for i in 0 ..< item.len:
        if i == 0 and item[i] == '-':
          continue
        if item[i] notin {'0' .. '9'}:
          raise newException(RedisProtocolError, "Bad BIGNUM value")
      result.bignum = item
    of REDIS_MESSAGE_NIL:
      if item.len != 0:
        raise newException(RedisProtocolError, "Bad NIL value")
    of REDIS_MESSAGE_ERROR:
      result.error = item
      raiseException(item)
    of REDIS_MESSAGE_SIMPLESTRING:
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
    if result.messageType == REDIS_MESSAGE_VERB and str.len < 6 and str[3] != ':':
      raise newException(RedisProtocolError, "Verbatim string 4 bytes of content type are missing or incorrectly encoded. length is wrong")

proc processAggregateItem(redis: Redis, resTyp: RedisMessageTypes, item: string): Future[RedisMessage] {.async.} =
  result = RedisMessage(messageType: resTyp)
  let arraySize = toBiggestInt(item)
  case resTyp
  of REDIS_MESSAGE_ARRAY, REDIS_MESSAGE_PUSH:
    result.arr = @[]
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        result.arr.add(await parseResponse(redis))
  of REDIS_MESSAGE_MAP:
    result.map = initTable[string, RedisMessage]()
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        let key = await redis.parseResponse(key = true)
        let value = await redis.parseResponse()
        result.map[key.str[]] = value
  of REDIS_MESSAGE_SET:
    result.hashSet = initHashSet[RedisMessage]()
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        result.hashSet.incl(await redis.parseResponse())
  of REDIS_MESSAGE_ATTRS:
    # It is really a map like MAP reply type, but we'll ignore it by now.
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        let key = await redis.parseResponse(key = true)
        let value = await redis.parseResponse()
  else:
    raise newException(RedisTypeError, "Wrong type for aggregate item")

proc encode[T: SomeSignedInt | SomeUnsignedInt](x: T): RedisMessage =
  result = RedisMessage(messageType: REDIS_MESSAGE_INTEGER, integer: x)
proc encode[T: float | float32 | float64](x: T): RedisMessage =
  result = RedisMessage(messageType: REDIS_MESSAGE_DOUBLE, double: x)
proc encode(x: string): RedisMessage =
  result = RedisMessage(messageType: REDIS_MESSAGE_STRING)
  result.str[] = x
proc encode(x: bool): RedisMessage =
  result = RedisMessage(messageType: REDIS_MESSAGE_BOOL, b: x)
proc encode[T](x: openArray[T]): RedisMessage =
  result = RedisMessage(messageType: REDIS_MESSAGE_ARRAY, arr: @[])
  for v in x:
    result.arr.add(x.encode())
