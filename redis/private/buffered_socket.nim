import std/[asyncdispatch, asyncnet, net]
import ptr_math

const DEFAULT_BUFFER_SIZE = 4096

type
  BufferedSocketBaseObj = object of RootObj
    buffer: seq[byte]
    pos: ptr byte
    dataSize: int
    freeSpace: int

  AsyncBufferedSocket* = ref AsyncBufferedSocketObj
  AsyncBufferedSocketObj* = object of BufferedSocketBaseObj
    sock: AsyncSocket
  
  BufferedSocket* = ref BufferedSocketObj
  BufferedSocketObj* = object of BufferedSocketBaseObj
    sock: Socket

proc resetPos(socket: AsyncBufferedSocket | BufferedSocket) =
  socket.pos = cast[ptr byte](addr(socket.buffer[0]))
  socket.dataSize = 0
  socket.freeSpace = socket.buffer.len

proc advancePos(socket: AsyncBufferedSocket | BufferedSocket, step: int) =
  if socket.pos + step >= socket.pos + socket.buffer.len - 1:
    socket.resetPos()
  else:
    if socket.dataSize >= step:
      socket.pos += step
      socket.dataSize -= step
    else:
      socket.resetPos()
  #echo "pos: ", socket.pos[0].char, ", socket.dataSize: ", socket.dataSize, ", socket.freeSpace: ", socket.freeSpace

proc newBufferedSocket*(socket: Socket = nil, bufSize = DEFAULT_BUFFER_SIZE): BufferedSocket =
  result.new
  if socket.isNil:
    result.sock = newSocket(buffered = false)
  else:
    result.sock = socket
  result.buffer.setLen(bufSize)
  result.resetPos()

proc newAsyncBufferedSocket*(socket: AsyncSocket = nil, bufSize = DEFAULT_BUFFER_SIZE): AsyncBufferedSocket =
  result.new
  if socket.isNil:
    result.sock = newAsyncSocket(buffered = false)
  else:
    result.sock = socket
  result.buffer.setLen(bufSize)
  result.resetPos()

proc connect*(sock: AsyncBufferedSocket | BufferedSocket, address: string, port: Port) {.multisync.} =
  await sock.sock.connect(address, port)

proc close*(sock: AsyncBufferedSocket | BufferedSocket) =
  sock.sock.close()

proc fetchMaxAvailable(sock: AsyncBufferedSocket | BufferedSocket) {.multisync.} =
  if sock.freeSpace == 0 and sock.dataSize == 0:
    sock.resetPos()
  if sock.freeSpace > 0:
    when sock is AsyncBufferedSocket:
        var dataSize = await sock.sock.recvInto(sock.pos+sock.dataSize, sock.freeSpace)
    else:
      var dataSize = sock.sock.recv(sock.pos+sock.dataSize, sock.freeSpace)
    sock.dataSize += dataSize
    sock.freeSpace -= dataSize
  #echo cast[seq[char]](sock.buffer)
  #echo "DataSize: ", sock.dataSize, " FreeSpace: ", sock.freeSpace

proc recvInto*(sock: AsyncBufferedSocket | BufferedSocket, dst: ptr byte, size: int): Future[int] {.multisync.} =
  # if buffer has less than size dataSize we should copy all the dataSize, reset the pos
  # and dataSize and download more bufLen bytes
  var partSize = 0
  while partSize < size:
    if sock.dataSize == 0:
      await sock.fetchMaxAvailable()
    let diffSize = size - partSize
    if sock.dataSize >= diffSize:
      copyMem(dst+partSize, sock.pos, diffSize)
      partSize += diffSize
      sock.advancePos(diffSize)
    else:
      copyMem(dst+partSize, sock.pos, sock.dataSize)
      partSize += sock.dataSize
      sock.resetPos()
  result = size

proc recvInto*(sock: AsyncBufferedSocket | BufferedSocket, dst: openArray[byte]): Future[int] {.multisync.} =
  result = await sock.recvInto(cast[ptr byte](unsafeAddr(dst[0])), dst.len)

proc recv*(sock: AsyncBufferedSocket | BufferedSocket, size: int): Future[string] {.multisync.} =
  result.setLen(size)
  let newSize = await sock.recvInto(cast[ptr byte](result[0].addr), size)
  if newSize != size:
    raise newException(IOError, "Sizes don't match")

proc recvLine*(sock: AsyncBufferedSocket | BufferedSocket, maxLen = DEFAULT_BUFFER_SIZE): Future[string] {.multisync.} =
  var lastR: bool = false
  var lastL: bool = false
  var len, fullLen: int = 0
  while fullLen < maxLen:
    if sock.dataSize == 0:
      await sock.fetchMaxAvailable()
    for i in 0 .. sock.dataSize-1:
      case (sock.pos+i)[]
      of '\r'.byte:
        lastR = true
      of '\L'.byte:
        lastL = true
        break
      else:
        if lastR:
          break
        len.inc()
    fullLen += len
    result.setLen(fullLen)
    copyMem(cast[ptr byte](unsafeAddr(result[0+fullLen-len])), sock.pos, len)
    sock.advancePos(len)
    len = 0
    if lastR:
      sock.advancePos(1)
    if lastL:
      sock.advancePos(1)
    if lastR:
      break
  if fullLen == 0:
    result = ""

proc send(sock: AsyncBufferedSocket | BufferedSocket, source: ptr byte, size: int): Future[int] {.multisync.} =
  when sock is AsyncBufferedSocket:
    await sock.sock.send(source, size)
  else:
    var left = size
    while left > 0:
      let sz = sock.sock.send(source, left)
      left -= sz
  result = size

proc send*(sock: AsyncBufferedSocket | BufferedSocket, data: openArray[byte]): Future[int] {.multisync.} =
  result = await sock.send(cast[ptr byte](unsafeAddr(data[0])), data.len)

proc send*(sock: AsyncBufferedSocket | BufferedSocket, data: string): Future[int] {.multisync.} =
  result = await sock.send(cast[ptr byte](unsafeAddr(data[0])), data.len)
