import std/[asyncdispatch, macros]

macro asyncFor*(iterClause: untyped, body: untyped): untyped =
  if iterClause.kind != nnkInfix or iterClause[0] != ident("in"):
    error("expected `x in y` after for")

  let cursor = iterClause[2]
  let itemName = iterClause[1]
  result = quote do:
    while true:
      let future = next(`cursor`)
      yield future
      if future.failed: 
        raise future.readError()
      let tu = future.read()
      if not tu[0]: break
      let `itemName` {.inject.} = tu[1]
      `body`
