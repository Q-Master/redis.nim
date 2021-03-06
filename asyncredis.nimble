# Package
description = "Pure Nim asyncronous driver for Redis DB"
version     = "0.8.2"
license     = "MIT"
author      = "Vladimir Berezenko <qmaster2000@gmail.com>"

# Dependencies
requires "nim >= 1.4.00", "networkutils >= 0.2"

task test, "tests":
  let tests = @["connection", "proto", "commands"]
  #let tests = @["commands"]
  for test in tests:
    echo "Running " & test & " test"
    try:
      exec "nim c -r tests/" & test & ".nim"
    except OSError:
      continue
