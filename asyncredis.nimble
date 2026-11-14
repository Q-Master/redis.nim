# Package
description = "Pure Nim asyncronous driver for Redis DB"
version     = "0.9.3"
license     = "MIT"
author      = "Vladimir Berezenko <qmaster2000@gmail.com>"
skipDirs      = @["tests"]

# Dependencies
requires "nim >= 2.0", "networkutils >= 0.7.2"

task test, "tests":
  let tests = @["connection", "proto", "commands", "exceptions"]
  for test in tests:
    echo "Running " & test & " test"
    try:
      exec "nim c -r tests/" & test & ".nim"
    except OSError:
      continue
