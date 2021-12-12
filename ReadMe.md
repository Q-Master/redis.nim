# Asyncronous Redis Pure Nim library
This is a asyncronous driver to use with [Redis Database](https://redis.io).

Driver features
-----------------
This table represents Redis features and their implementation status within
`redis.redis` Nim module.

| Commands    | Status             |  Notes |
|------------:|:-------------------|:-------|
| Connection  | Tested and working | Some commands not tested |
| String      | Tested and working | Not tested: timeouts, scan |
| Keys        | Tested and working | Not tested: timeouts, sort, scan |
| Lists       | Tested and working | Not tested: blocking ops, scan |
| Hashes      | Tested and working | Not tested: scan |
| Sets        | Tested and working | Not tested: scan |
| Sorted Sets | Tested and working | Not tested: scan |
| Scripting   | Not tested         | Not stable yet |
| Server      | Tested and working | Only FLUSHALL command implemented yet |

## Info
Driver is not yet fully complete and tested, but seems working. Those commands marked not tested are implemented, but tests need to be written.
Some code seems a bit "hacky" but working.

## Examples
All the needed examples are almost self-explanatory in [commands.nim](tests/commands.nim)
