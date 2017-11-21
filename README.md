# Sharded Redis Client

## Description

Creates a Redis client that consistently shards by key using sha1

## Usage

### Example 1:
```
const shardedClient = new ShardedRedisClient([{
  host: 'box1.redis',
  port_range: [6370, 6372]
}, {
  host: 'box2.redis',
  port_range: [10000]
}, {
  host: 'box3.redis',
  port_range: [6379, 6380],
  slaveHosts: ['box3.redis.slave1', 'box3.redis.slave2'],
  readPreference: 'slave' // <- force read slave on this set
}]);
```

Represents a 6-client sha1 HashRing in the following order:
```
[
 'box1.redis:6370',
 'box1.redis:6371',
 'box1.redis:6372',
 'box2.redis:10000',
 'box3.redis:6379', // slaves: 'box3.redis.slave1:6379', 'box3.redis.slave2:6379'
 'box3.redis:6380', // slaves: 'box3.redis.slave1:6380', 'box3.redis.slave2:6380'
]
```

### Example 2 (single client):
```
const shardedClient = new ShardedRedisClient([{ host: 'localhost', port_range: [3000] }])
```

Represents a 1-client (unsharded) HashRing that is only useful in development: `['localhost:3000']`

Use with slaves: `shardedClient.slaveOk()`

If a slave doesn't exist for a given client, nothing changes.
Also, `slaveOk()` only affects read-only commands.

## Contributing

Pull requests are welcome, please open an issue first.

## Tests

Run tests using...

```
$ npm test
```

## Copyright

Copyright © 2013 - 2017 Tinder, Inc.

Permission to use, copy, modify, and distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
