# Asyncronous Redis Pure Nim library
This is a asyncronous driver to use with [Redis Database](https://redis.io).

AsyncRedis features
-----------------
Library almost completely implements common use features including authorization, key commands,
lists, hashes, sets ans sorted sets.
There's a table representing implementation status within
`asyncredis` Nim module.

| Commands    | Status             |  Notes |
|------------:|:-------------------|:-------|
| Connection  | Tested and working | |
| String      | Tested and working | |
| Keys        | Tested and working | Not tested: sort |
| Lists       | Tested and working | |
| Hashes      | Tested and working | |
| Sets        | Tested and working | |
| Sorted Sets | Tested and working | |
| Scripting   | Not tested         | Not stable yet |
| Server      | Tested and working | Only FLUSHALL command implemented yet |

## Info
Driver is not yet fully complete and tested, but seems working. Those commands marked not tested are implemented, but tests need to be written.
Some code seems a bit "hacky" but working.

## Examples
All the needed examples are almost self-explanatory in [commands.nim](tests/commands.nim)
