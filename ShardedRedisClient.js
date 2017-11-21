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
const async = require('async');
const assert = require('assert');
const crypto = require('crypto');
const EventEmitter = require('events').EventEmitter;
const ShardSet = require('./lib/ShardSet');
const HostRange = require('./lib/HostRange');
const WrappedClient = require('./lib/WrappedClient');

const READ_ONLY = [
  'sismember',
  'get',
  'mget',
  'exists',
  'hgetall',
  'llen',
  'lrange',
  'scard',
  'smembers',
  'srandmember',
  'sunion', // sunion assumes that each key is on the same shard
  'zrevrange',
  'zcard',
  'zcount',
  'zscore',
  'zrange',
  'zrangebyscore'
];

const SHARDABLE = [
  'sadd',
  'expire',
  'ttl',
  'sismember',
  'srem',
  'get',
  'mget',
  'exists',
  'scard',
  'smembers',
  'sunion', // sunion assumes that each key is on the same shard
  'hdel',
  'hget',
  'hincrby',
  'hset',
  'hmset',
  'hgetall',
  'llen',
  'lpush',
  'lrange',
  'ltrim',
  'set',
  'setnx',
  'setex',
  'psetex',
  'del',
  'srandmember',
  'zrevrange',
  'incr',
  'zadd',
  'zcard',
  'zcount',
  'zrem',
  'zscore',
  'zrange',
  'zrangebyscore',
  'zremrangebyrank',
  'zremrangebyscore'
];

function getClientIndex(key, wrappedClients) {
  const hash = crypto.createHash('sha1').update(String(key)).digest('hex');
  const hashCut = hash.substring(0, 4);
  const hashNum = parseInt(hashCut, 16);

  return hashNum % wrappedClients.length;
}

function getReadClient(clientIndex, wrappedClients, readSlave) {
  const wrappedClient = wrappedClients[clientIndex];
  const slaveOk = readSlave || wrappedClient.readPreference === 'slave';

  return slaveOk ? wrappedClient.getSlave() : wrappedClient.get();
}

function getWriteClient(clientIndex, wrappedClients) {
  const wrappedClient = wrappedClients[clientIndex];

  return wrappedClient.get();
}

function findMasterClient(key, wrappedClients) {
  const clientIndex = getClientIndex(key, wrappedClients);
  const wrappedClient = wrappedClients[clientIndex];

  return wrappedClient.get();
}

function getWrappedClient(key, wrappedClients) {
  const clientIndex = getClientIndex(key, wrappedClients);

  return wrappedClients[clientIndex];
}

function findMatchedClient(key, cmd, wrappedClients, readSlave) {
  const clientIndex = getClientIndex(key, wrappedClients);
  const isReadCmd = READ_ONLY.indexOf(cmd) >= 0;

  return isReadCmd ? getReadClient(clientIndex, wrappedClients, readSlave) : getWriteClient(clientIndex, wrappedClients);
}

class ShardedRedisClient extends EventEmitter {
  
  constructor(configurationArray, options) {
    assert(Array.isArray(configurationArray), 'first argument \'configurationArray\' must be an array.');

    if (typeof options === 'boolean')
      options = { usePing: options };
    else if (!options)
      options = {};

    if (options.usePing !== false)
      options.usePing = true;

    const hostRanges = configurationArray.map((hostRangeConfig) =>
      new HostRange(hostRangeConfig.host, hostRangeConfig.port_range[0], hostRangeConfig.port_range[1],
        hostRangeConfig.slaveHosts, hostRangeConfig.readPreference));

    const shardSet = new ShardSet(hostRanges);
    const wrappedClients = shardSet.toArray().map((conf) => new WrappedClient(conf, options));

    super();

    this._usePing = options.usePing;
    this._readSlave = false;
    this._wrappedClients = wrappedClients;
    this._ringSize = wrappedClients.length;
    this._readTimeout = options.readTimeout;
    this._writeTimeout = options.writeTimeout;
    this._CircuitBreaker = options.breakerConfig;

    // To support old tests
    this._getWrappedClient = (key) => getWrappedClient(key, this._wrappedClients);
  }

  slaveOk() {
    const props = {
      _readSlave: {
        value: true
      }
    };

    return Object.create(this, props);
  }

  setReadTimeOut(timeoutVal) {
    const props = {
      _readTimeout: {
        value: timeoutVal
      }
    };

    return Object.create(this, props);
  }

  setWriteTimeOut(timeoutVal) {
    const props = {
      _writeTimeout: {
        value: timeoutVal
      }
    };

    return Object.create(this, props);
  }

  userCircuitBreaker(config) {
    const props = {
      _CircuitBreakerConfig: config
    };

    return Object.create(this, props);
  }

  multi(key, multiArr) {
    const client = findMatchedClient(key, 'multi', this._wrappedClients, this._readSlave);

    return client.multi(multiArr);
  }

  zaddMulti(key, arr, cb) {
    const client = findMatchedClient(key, 'zadd', this._wrappedClients, this._readSlave);
    let timeout = null;

    cb = _.once(cb);

    if (this._writeTimeout)
      timeout = setTimeout(cb, this._writeTimeout, new Error('Redis call timed out'));

    const wrappedCb = (err, results) => clearTimeout(timeout) || cb(err, results);

    return client.zadd([key].concat(arr), wrappedCb);
  }

  zremMulti(key, arr, cb) {
    const client = findMatchedClient(key, 'zrem', this._wrappedClients, this._readSlave);
    let timeout = null;

    cb = _.once(cb);

    if (this._writeTimeout)
      timeout = setTimeout(cb, this._writeTimeout, new Error('Redis call timed out'));

    const wrappedCb = (err, results) => clearTimeout(timeout) || cb(err, results);

    return client.zrem([key].concat(arr), wrappedCb);
  }

  keys(pattern, done) {
    const readClients = this._wrappedClients.map((c, i) => getReadClient(i, this._wrappedClients, this._readSlave));
    const allKeys = {};

    async.each(readClients, (rc, cb) => {
      rc.keys(pattern, (err, keys) => {
        if (err)
          return cb(err);

        keys.forEach((k) => allKeys[k] = true);

        cb();
      });
    }, (err) => {
      if (err)
        return done(err, []);

      done(null, Object.keys(allKeys));
    });
  }

};

SHARDABLE.forEach((cmd) => {

  ShardedRedisClient.prototype[cmd + 'WithOptions'] = function () {
    // remove options from arguments to pass on to redis function
    const options = Array.prototype.shift.call(arguments);

    // continue with original arguments w/o options
    const args = arguments;
    const key = Array.isArray(arguments[0]) ? arguments[0][0] : arguments[0];

    // find client based on sharding key, not storage key
    const shardKey = (options && options['shardKey']) || key;
    let client = findMatchedClient(shardKey, cmd, this._wrappedClients, this._readSlave);

    const startIndex = client._rrindex;
    const wrappedClient = getWrappedClient(shardKey, this._wrappedClients);

    let mainCb = args[args.length - 1];

    if (typeof mainCb !== 'function')
      mainCb = args[args.length] = _.noop;

    mainCb = _.once(mainCb);

    const isReadCmd = READ_ONLY.indexOf(cmd) !== -1;
    const timeout = isReadCmd ? this._readTimeout : this._writeTimeout;

    const _this = this;
    let timeoutHandler = null;
    let breaker = client._breaker;

    const timeoutCb = args[args.length - 1] = function (err) {
      if (timeoutHandler)
        clearTimeout(timeoutHandler);

      if (!err) {
        if (breaker)
          breaker.pass();

        return mainCb.apply(this, arguments);
      }

      // For now, both emit and log. Eventually, logging will be removed
      _this.emit('err', new Error(`sharded-redis-client [${client.address}] err: ${err}`));
      console.error(new Date().toISOString(), `sharded-redis-client [${client.address }] err: ${err}`);

      if (breaker && err.message !== 'breaker open')
        breaker.fail();

      if (!client._isMaster) {
        client = wrappedClient.slaves.next(client);

        if (client._rrindex == startIndex)
          client = findMasterClient(shardKey, _this._wrappedClients);

        breaker = client._breaker;

        return wrappedCmd(client, args);
      }

      mainCb.apply(this, arguments);
    };

    wrappedCmd(client, args);

    function wrappedCmd(client, args) {
      if (!breaker || breaker.closed()) {
        // Intentionally don't do this if timeout was set to 0
        if (timeout)
          timeoutHandler = setTimeout(timeoutCb, timeout, new Error('Redis call timed out'));

        return client[cmd].apply(client, args);
      }

      timeoutCb(new Error('breaker open'));
    }
  }

  ShardedRedisClient.prototype[cmd] = function () {
    const args = arguments;

    // add options as first param for backwards compatibility
    Array.prototype.unshift.call(args, {});

    // call new method with options in first param
    this[cmd + 'WithOptions'].apply(this, args);
  };

});

module.exports = ShardedRedisClient;
module.exports.READ_ONLY = READ_ONLY;
module.exports.SHARDABLE = SHARDABLE;
