# Package
description = "Pure Nim asyncronous driver for Redis DB"
version     = "0.8"
license     = "MIT"
author      = "Vladimir Berezenko <qmaster2000@gmail.com>"

# Dependencies
requires "nim >= 1.4.00", "ptr_math"

task test, "tests":
  let tests = @["connection", "proto", "commands"]
  #let tests = @["commands"]
  for test in tests:
    echo "Running " & test & " test"
    try:
      exec "nim c -r tests/" & test & ".nim"
    except OSError:
      continue
