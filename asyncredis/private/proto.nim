import std/asyncdispatch
import std/[strutils, sets, tables, hashes, json, options, sha1]
import ./connection
import ./exceptions

type
  RedisMessageKind* = enum
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

  RedisSortOrder* = enum
    REDIS_SORT_NONE,
    REDIS_SORT_ASC,
    REDIS_SORT_DESC
  
  RedisExpireType* = enum
    REDIS_EXPIRE_NOT_SET
    REDIS_EXPIRE_NX = "NX"
    REDIS_EXPIRE_XX = "XX"
    REDIS_EXPIRE_GT = "GT"
    REDIS_EXPIRE_LT = "LT"

  RedisMessage* = ref RedisMessageObj
  RedisMessageObj* = object of RootObj
    case kind*: RedisMessageKind
    of REDIS_MESSAGE_ERROR:
      error*: string
    of REDIS_MESSAGE_INTEGER:
      integer*: int64
    of REDIS_MESSAGE_DOUBLE:
      double*: float
    of REDIS_MESSAGE_NIL:
      discard
    of REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_STRING, REDIS_MESSAGE_VERB:
      str*: Option[string]
    of REDIS_MESSAGE_ARRAY, REDIS_MESSAGE_PUSH:
      arr*: seq[RedisMessage]
    of REDIS_MESSAGE_MAP:
      map*: Table[string, RedisMessage]
    of REDIS_MESSAGE_SET:
      hashSet*: HashSet[RedisMessage]
    of REDIS_MESSAGE_BOOL:
      boolean*: bool
    of REDIS_MESSAGE_BIGNUM:
      bignum*: string
    of REDIS_MESSAGE_ATTRS:
      discard
    
proc encodeRedis*[T: SomeSignedInt | SomeUnsignedInt](x: T): RedisMessage
proc encodeRedis*[T: float | float32 | float64](x: T): RedisMessage
proc encodeRedis*(x: string): RedisMessage
proc encodeRedis*(x: bool): RedisMessage
proc encodeRedis*(x: SecureHash): RedisMessage
proc encodeRedis*[T](x: openArray[T]): RedisMessage
proc encodeRedis*[T](x: tuple[a: string, b: T]): RedisMessage
proc encodeRedis*[T](x: array[0..0, (string, T)]): RedisMessage

proc processLineItem(resTyp: RedisMessageKind, item: string, key: bool): RedisMessage
proc processBulkItem(redis: RedisConn, resTyp: RedisMessageKind, item: string, key: bool): Future[RedisMessage] {.async.}
proc processAggregateItem(redis: RedisConn, resTyp: RedisMessageKind, item: string): Future[RedisMessage] {.async.}
proc parseResponse*(redis: RedisConn, key: bool = false): Future[RedisMessage] {.async.} =
  while true:
    # We are skipping the ATTRIBUTE type of redis reply always.
    let response = await redis.readLine()
    var tp: RedisMessageKind
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
      break
    of REDIS_MESSAGE_STRING, REDIS_MESSAGE_VERB:
      result = await processBulkItem(redis, tp, response[1 .. ^1], key);
      break
    of REDIS_MESSAGE_ARRAY, REDIS_MESSAGE_MAP, REDIS_MESSAGE_SET:
      result = await processAggregateItem(redis, tp, response[1 .. ^1]);
      break
    of REDIS_MESSAGE_ATTRS, REDIS_MESSAGE_PUSH:
      let _ = await processAggregateItem(redis, tp, response[1 .. ^1]);

template encodeString(str: Option[string], prefix = "$") =
  if str.isSome:
    let s = str.get()
    result.add(prefix & $s.len)
    result.add(s)
  else:
    result.add(prefix & "-1")

template encodeString(str: string, prefix = "$") =
  result.add(prefix & $str.len)
  result.add(str)

proc prepareRequest*(request: RedisMessage): seq[string] =
  result = @[]
  case request.kind
  of REDIS_MESSAGE_INTEGER:
    result.add(":" & $request.integer)
  of REDIS_MESSAGE_DOUBLE:
    result.add("," & $request.double)
  of REDIS_MESSAGE_SIMPLESTRING:
    if request.str.isSome:
      result.add("+" & request.str.get())
    else:
      result.add("+" & "")
  of REDIS_MESSAGE_STRING:
    request.str.encodeString()
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
    result.add(if request.boolean: "t" else: "f")
  of REDIS_MESSAGE_VERB:
    request.str.encodeString("=")
  of REDIS_MESSAGE_BIGNUM:
    result.add("(" & request.bignum)
  else:
    raise newException(RedisTypeError, "Unsupported type for outgoing requests")

proc toText(result: var string, redisMessage: RedisMessage, nextLine = true, currIndent = 0, isArray = false)

proc `$`*(redisMessage: RedisMessage): string =
  result = ""
  toText(result, redisMessage, false)

proc pretty*(redisMessage: RedisMessage): string =
  result = ""
  toText(result, redisMessage, true)

proc toJson*(redisMessage: RedisMessage): JsonNode =
  case redisMessage.kind
  of REDIS_MESSAGE_STRING, REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_VERB:
    if redisMessage.str.isSome:
      result = newJString(redisMessage.str.get())
    else:
      result = newJNull()
  of REDIS_MESSAGE_ERROR:
    result = newJString(redisMessage.error)
  of REDIS_MESSAGE_INTEGER:
    result = newJInt(redisMessage.integer)
  of REDIS_MESSAGE_DOUBLE:
    result = newJFloat(redisMessage.double)
  of REDIS_MESSAGE_BOOL:
    result = newJBool(redisMessage.boolean)
  of REDIS_MESSAGE_NIL:
    result = newJNull()
  of REDIS_MESSAGE_ARRAY, REDIS_MESSAGE_PUSH:
    result = newJArray()
    for subMsg in redisMessage.arr:
      result.add(subMsg.toJson())
  of REDIS_MESSAGE_SET:
    result = newJArray()
    for subMsg in redisMessage.hashSet:
      result.add(subMsg.toJson())
  of REDIS_MESSAGE_MAP:
    result = newJObject()
    for k,v in redisMessage.map:
      result[k] = v.toJson()
  of REDIS_MESSAGE_BIGNUM:
    result = newJString(redisMessage.bignum)
  of REDIS_MESSAGE_ATTRS:
    result = newJNull()

#------- pvt

proc hash*(msg: RedisMessage): Hash =
  result = cast[Hash](cast[uint](msg) shr 3)

template toBiggestInt(item: string): BiggestInt =
  let tmp = parseBiggestInt(item)
  if tmp < low(int64) or tmp > high(int64):
    raise newException(RedisProtocolError, "Bad INTEGER value")
  tmp

proc processLineItem(resTyp: RedisMessageKind, item: string, key: bool): RedisMessage =
  if key:
    result = RedisMessage(kind: REDIS_MESSAGE_STRING)
    result.str = item.option
  else:
    result = RedisMessage(kind: resTyp)
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
      result.boolean = item in ["T", "t"]
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
      result.str = item.option()
    else:
      raise newException(RedisTypeError, "Wrong type for line item")

proc processBulkItem(redis: RedisConn, resTyp: RedisMessageKind, item: string, key: bool): Future[RedisMessage] {.async.} =
  result = RedisMessage(kind: resTyp)
  let stringSize = toBiggestInt(item)
  if stringSize == -1:
    if key:
      raise newException(RedisProtocolError, "Map key can't be nil")
    result.str = string.none()
  else:
    let str = await redis.readRawString((stringSize+2).int)
    let s = str[0 .. ^3]
    result.str = s.option()
    if s.len != stringSize:
      raise newException(RedisProtocolError, "String/verb length is wrong")
    if result.kind == REDIS_MESSAGE_VERB and str.len < 6 and str[3] != ':':
      raise newException(RedisProtocolError, "Verbatim string 4 bytes of content type are missing or incorrectly encoded. length is wrong")

proc processAggregateItem(redis: RedisConn, resTyp: RedisMessageKind, item: string): Future[RedisMessage] {.async.} =
  result = RedisMessage(kind: resTyp)
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
        result.map[key.str.get] = value
  of REDIS_MESSAGE_SET:
    result.hashSet = initHashSet[RedisMessage]()
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        result.hashSet.incl(await redis.parseResponse())
  of REDIS_MESSAGE_ATTRS:
    # It is really a map like MAP reply type, but we'll ignore it by now.
    if arraySize != -1:
      for _ in 0 ..< arraySize:
        let key {.used.} = await redis.parseResponse(key = true)
        let value {.used.} = await redis.parseResponse()
  else:
    raise newException(RedisTypeError, "Wrong type for aggregate item")

proc encodeRedis*[T: SomeSignedInt | SomeUnsignedInt](x: T): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_INTEGER, integer: x)
proc encodeRedis*[T: float | float32 | float64](x: T): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_DOUBLE, double: x)
proc encodeRedis*(x: string): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_STRING)
  result.str = x.option
proc encodeRedis*(x: bool): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_BOOL, boolean: x)
proc encodeRedis*(x: SecureHash): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_STRING, str: ($x).option)
proc encodeRedis*[T](x: openArray[T]): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_ARRAY, arr: @[])
  for v in x:
    result.arr.add(v.encodeRedis())
proc encodeRedis*[T](x: tuple[a: string, b: T]): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_MAP, map: initTable[string, RedisMessage]())
  result.map[x.a] = x.b.encodeRedis
proc encodeRedis*[T](x: array[0..0, (string, T)]): RedisMessage =
  result = RedisMessage(kind: REDIS_MESSAGE_MAP, map: initTable[string, RedisMessage]())
  result.map[x[0][0]] = x[0][1].encodeRedis()

const INDENT = 2

proc indent(s: var string, i: int, nextLine: bool) =
  if nextLine:
    s.add(spaces(i))

proc incIndent(curr: int, nextLine: bool): int =
  if nextLine: 
    result = curr + INDENT
  else:
    result = INDENT

proc nl(s: var string, nextLine: bool) =
  s.add(if nextLine: "\n" else: " ")

proc escapeString(s: string; result: var string) =
  result.add("\"")
  for c in s:
    case c
    of '\L': 
      result.add("\\n")
    of '\b': 
      result.add("\\b")
    of '\f': 
      result.add("\\f")
    of '\t': 
      result.add("\\t")
    of '\v': 
      result.add("\\u000b")
    of '\r': 
      result.add("\\r")
    of '"': 
      result.add("\\\"")
    of '\0'..'\7': 
      result.add("\\u000" & $ord(c))
    of '\14'..'\31': 
      result.add("\\u00" & toHex(ord(c), 2))
    of '\\': 
      result.add("\\\\")
    else: 
      result.add(c)
  result.add("\"")

proc toText(result: var string, redisMessage: RedisMessage, nextLine = true, currIndent = 0, isArray = false) =
  case redisMessage.kind
  of REDIS_MESSAGE_STRING, REDIS_MESSAGE_SIMPLESTRING, REDIS_MESSAGE_VERB:
    if isArray: 
      result.indent(currIndent, nextLine)
    escapeString(redisMessage.str.get, result)
  of REDIS_MESSAGE_INTEGER:
    if isArray: 
      result.indent(currIndent, nextLine)
    result.add($redisMessage.integer)
  of REDIS_MESSAGE_DOUBLE:
    if isArray: 
      result.indent(currIndent, nextLine)
    result.add($redisMessage.double)
  of REDIS_MESSAGE_BOOL:
    if isArray: 
      result.indent(currIndent, nextLine)
    result.add(if redisMessage.boolean: "true" else: "false")
  of REDIS_MESSAGE_NIL:
    if isArray: 
      result.indent(currIndent, nextLine)
    result.add("nil")
  of REDIS_MESSAGE_BIGNUM:
    if isArray: 
      result.indent(currIndent, nextLine)
    result.add(redisMessage.bignum)
  of REDIS_MESSAGE_ERROR:
    if isArray: 
      result.indent(currIndent, nextLine)
    escapeString(redisMessage.error, result)
  of REDIS_MESSAGE_ARRAY, REDIS_MESSAGE_PUSH:
    if isArray: 
      result.indent(currIndent, nextLine)
    if redisMessage.arr.len != 0:
      result.add("[")
      result.nl(nextLine)
      var addSep: bool = false
      for e in redisMessage.arr:
        if addSep:
          result.add(",")
          result.nl(nextLine)
        addSep = true
        toText(result, e, nextLine, incIndent(currIndent, nextLine), true)
      result.nl(nextLine)
      result.indent(currIndent, nextLine)
      result.add("]")
    else: 
      result.add("[]")
  of REDIS_MESSAGE_MAP:
    if isArray: 
      result.indent(currIndent, nextLine)
    if redisMessage.map.len != 0:
      result.add("{")
      result.nl(nextLine)
      var addSep: bool = false
      for k,v in redisMessage.map:
        if addSep:
          result.add(",")
          result.nl(nextLine)
        addSep = true
        result.indent(incIndent(currIndent, nextLine), nextLine)
        escapeString(k, result)
        result.add(": ")
        toText(result, v, nextLine, incIndent(currIndent, nextLine))
      result.nl(nextLine)
      result.indent(currIndent, nextLine) # indent the same as {
      result.add("}")
    else:
      result.add("{}")
  of REDIS_MESSAGE_SET:
    if isArray: 
      result.indent(currIndent, nextLine)
    if redisMessage.hashSet.len != 0:
      result.add("{")
      result.nl(nextLine)
      var addSep: bool = false
      for e in redisMessage.hashSet:
        if addSep:
          result.add(",")
          result.nl(nextLine)
        addSep = true
        toText(result, e, nextLine, incIndent(currIndent, nextLine), true)
      result.nl(nextLine)
      result.indent(currIndent, nextLine)
      result.add("}")
    else: 
      result.add("{}")
  else:
    discard
