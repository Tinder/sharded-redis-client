/**
 * Copyright © 2013 - 2017 Tinder, Inc.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

'use strict';

const _ = require('lodash');
const EventEmitter = require('events').EventEmitter;

const cb = () => function () {
  const callback = arguments[arguments.length - 1];

  if (typeof callback === 'function')
    callback();
};

class MockRedisClient extends EventEmitter {

  constructor(port, host) {
    super();

    this.port = port;
    this.host = host;
    this.address = `${host}:${port}`;
  }

  _ready() {
    this.emit('ready');
  }

};

_.extend(MockRedisClient.prototype, {
  sadd: cb(),
  expire: cb(),
  ttl: cb(),
  sismember: cb(),
  srem: cb(),
  get: cb(),
  mget: cb(),
  exists: cb(),
  scard: cb(),
  smembers: cb(),
  sunion: cb(),
  hdel: cb(),
  hget: cb(),
  hincrby: cb(),
  hset: cb(),
  hmset: cb(),
  hgetall: cb(),
  llen: cb(),
  lpush: cb(),
  lrange: cb(),
  ltrim: cb(),
  set: cb(),
  setnx: cb(),
  setex: cb(),
  psetex: cb(),
  del: cb(),
  srandmember: cb(),
  zrevrange: cb(),
  incr: cb(),
  zadd: cb(),
  zcard: cb(),
  zcount: cb(),
  zrem: cb(),
  zscore: cb(),
  zrange: cb(),
  zrangebyscore: cb(),
  zremrangebyrank: cb(),
  multi: cb(),
  zaddmulti: cb(),
  zremmulti: cb(),
  keys: cb(),
  slaveOk: cb(),
  zremrangebyscore: cb()
});

module.exports = MockRedisClient;
module.exports.createClient = (port, host, options) => new MockRedisClient(port, host, options);
