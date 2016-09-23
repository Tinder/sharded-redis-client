var redis = require('redis');
var crypto = require('crypto');
var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var async = require('async');
var CircuitBreaker = require('circuit-breaker');

module.exports = ShardedRedisClient;

/**
 * Creates a dummy redis client that consistently shards by key using sha1
 * @param {configurationArray} array - list of "HostRange" configuration objects
 * @param {options} Object - Object of optional configuration info. Available options:
 *                           - usePing: a Boolean describing whether or not to try pinging the redis client every so often.
 *                           - readTimeout: Number - amount of time to wait for redis before calling back without out the response on a read command.
 *                                                   A value of 0 will be treated as having readTimeouts turned off. Defaults to 0.
 *                           - writeTimeout: Number - amount of time to wait for redis before calling back without out the response on a write command.
 *                                                    A value of 0 will be treated as having writeTimeouts turned off. Defaults to 0
 *                           - breakerConfig: an Object with configuration values for circuit breakers
 *                           - redisOptions: an Object to pass as options to the redis client
 *
 * ex 1:
 *  new ShardedRedisClient([
 *    { 'host' : 'box1.redis' , port_range : [6370, 6372] },
 *    { 'host' : 'box2.redis' , port_range : [10000] },
 *    { 'host' : 'box3.redis' , port_range : [6379, 6380],
 *        slaveHosts:["box3.redis.slave1","box3.redis.slave2"], readPreference: "slave" }, // <- force read slave on this set
 *  ])
 *
 * represents a 6-client sha1 HashRing in the following order:
 *
 *  [
 *    "box1.redis:6370" ,
 *    "box1.redis:6371" ,
 *    "box1.redis:6372" ,
 *    "box2.redis:10000" ,
 *    "box3.redis:6379" , // slaves: "box3.redis.slave1:6379" , "box3.redis.slave2:6379"
 *    "box3.redis:6380" , // slaves: "box3.redis.slave1:6380" , "box3.redis.slave2:6380"
 *  ]
 *
 * ex 2 (single client):
 *  var shardedClient = new ShardedRedisClient([{ 'host' : 'localhost' , port_range : [3000] }])
 *
 * represents a 1-client (unsharded) HashRing that is only useful in development:
 * [ "localhost:3000" ]
 *
 * use with slaves: shardedClient.slaveOk()
 *
 * if a slave doesn't exist for a given client, nothing changes.
 * Also, slaveOk() only affects read-only commands.
 *
 *
 */

var shardable = [
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
  'zremrangebyrank'
];

var readOnly = [
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

ShardedRedisClient.prototype.__proto__ = EventEmitter.prototype;

WrappedClient.prototype.__proto__ = EventEmitter.prototype;

function ShardedRedisClient(configurationArray, options) {

  // Put this here for (temporary) backwards compatibility
  if (typeof options === 'boolean')
    options = { usePing: options };
  else if (!options)
    options = {};

  if (options.usePing !== false) options.usePing = true;

  assert(Array.isArray(configurationArray), 'first argument \'configurationArray\' must be an array.');

  var _this = this;
  var hostRanges = configurationArray.map(function (hostRangeConfig) {
    return new HostRange(hostRangeConfig.host, hostRangeConfig.port_range[0], hostRangeConfig.port_range[1],
      hostRangeConfig.slaveHosts, hostRangeConfig.readPreference);
  });

  var shardSet = new ShardSet(hostRanges);
  var wrappedClients = shardSet.toArray().map(function (conf) {
    return new WrappedClient(conf, options);
  });

  Object.defineProperty(_this, '_usePing', { value: options.usePing });
  Object.defineProperty(_this, '_readSlave', { value: false });
  Object.defineProperty(_this, '_wrappedClients', { value: wrappedClients });
  Object.defineProperty(_this, '_ringSize', { value: wrappedClients.length });
  Object.defineProperty(_this, '_readTimeout', { value: options.readTimeout });
  Object.defineProperty(_this, '_writeTimeout', { value: options.writeTimeout });
  Object.defineProperty(_this, '_CircuitBreaker', { value: options.breakerConfig });

  //Object.defineProperty(_this, '_redisOptions', { value: options.redisOptions || {} });
}

ShardedRedisClient.prototype.slaveOk = function () {
  var _this = this;
  return Object.create(_this, {
    _readSlave: { value: true }
  });
};

ShardedRedisClient.prototype.setReadTimeOut = function (timeoutVal) {
  var _this = this;
  return Object.create(_this, {
    _readTimeout: { value: timeoutVal }
  });
};

ShardedRedisClient.prototype.setWriteTimeOut = function (timeoutVal) {
  var _this = this;
  return Object.create(_this, {
    _writeTimeout: { value: timeoutVal }
  });
};

ShardedRedisClient.prototype.useCircuitBreaker = function (config) {
  var _this = this;
  return Object.create(_this, {
    _CircuitBreakerConfig: config
  });
};

ShardedRedisClient.prototype._findMatchedClient = function (key, cmd) {
  key = key.toString();
  var clientIndex = this._getClientIndex(key);
  var isReadCmd = readOnly.indexOf(cmd) >= 0;
  return isReadCmd ? this._getReadClient(clientIndex) : this._getWriteClient(clientIndex);
};

ShardedRedisClient.prototype._findMasterClient = function (key) {
  key = key.toString();
  var clientIndex = this._getClientIndex(key);
  var wrappedClient = this._wrappedClients[clientIndex];
  return wrappedClient.get();
};

ShardedRedisClient.prototype._getReadClient = function (clientIndex) {
  var wrappedClient = this._wrappedClients[clientIndex];
  var slaveOk = this._readSlave || wrappedClient.readPreference == 'slave';
  return slaveOk ? wrappedClient.getSlave() : wrappedClient.get();
};

ShardedRedisClient.prototype._getWriteClient = function (clientIndex) {
  var wrappedClient = this._wrappedClients[clientIndex];
  return wrappedClient.get();
};

ShardedRedisClient.prototype._getWrappedClient = function (key) {
  var clientIndex = this._getClientIndex(key);
  return this._wrappedClients[clientIndex];
};

ShardedRedisClient.prototype._getClientIndex = function (key) {
  return getNode(key, this._wrappedClients.length);
};

shardable.forEach(function (cmd) {

  // TODO: check that this works
  // TODO: sharding key passed as separate arg
  ShardedRedisClient.prototype[cmd + 'WithOptions'] = function (/* arguments */) {
    var _this = this;
    // remove options from arguments to pass on to redis function
    var options = Array.prototype.shift.call(arguments);

    // continue with original arguments w/o options
    var args = arguments;
    var key = Array.isArray(arguments[0]) ? arguments[0][0] : arguments[0];

    // find client based on sharding key, not storage key
    var shardKey = (options && options['shardKey']) || key;
    var client = this._findMatchedClient(shardKey, cmd);

    var startIndex = client._rrindex;
    var wrappedClient = _this._getWrappedClient(shardKey);

    var mainCb = args[args.length - 1];
    if (typeof mainCb !== 'function') mainCb = args[args.length] = noop;
    mainCb = once(mainCb);

    var isReadCmd = readOnly.indexOf(cmd) >= 0;
    var timeout = isReadCmd ? _this._readTimeout : _this._writeTimeout;

    var breaker = client._breaker;
    var timeoutHandler;
    var timeoutCb = args[args.length - 1] = function (err) {
      if (timeoutHandler) clearTimeout(timeoutHandler);
      if (!err) {
        if (breaker) breaker.pass();
        mainCb.apply(this, arguments);
        return;
      }

      // For now, both emit and log. Eventually, logging will be removed
      _this.emit('err', new Error('sharded-redis-client [' + client.address + '] err: ' + err));
      console.error(new Date().toISOString(), 'sharded-redis-client [' + client.address + '] on command ' + cmd + ' err: ' + err);
      if (breaker && err.message !== 'breaker open') breaker.fail();

      if (!client._isMaster) {
        client = wrappedClient.slaves.next(client);
        if (client._rrindex == startIndex) {
          client = _this._findMasterClient(shardKey);
        }

        breaker = client._breaker;
        return wrappedCmd(client, args);
      }

      //var argmnts = Array.prototype.slice.call(arguments);
      //if (argmnts.length <= 2) argmnts[2] = client;
      mainCb.apply(this, arguments);
    };

    wrappedCmd(client, args);

    function wrappedCmd(client, args) {
      if (!breaker || breaker.closed()) {
        // Intentionally don't do this if timeout was set to 0
        if (timeout) timeoutHandler = setTimeout(timeoutCb, timeout, new Error('Redis call timed out'));

        return client[cmd].apply(client, args);
      }

      timeoutCb(new Error('breaker open'));
    }
  }

  ShardedRedisClient.prototype[cmd] = function (/* arguments */) {
    var _this = this;
    var args = arguments;
    // add options as first param for backwards compatibility
    Array.prototype.unshift.call(args, {});
    // call new method with options in first param
    _this[cmd + 'WithOptions'].apply(_this, args);
  };

});

/* Intentionally opinionated implementations of a few methods: */

ShardedRedisClient.prototype.multi = function (key, multiArr) {
  var client = this._findMatchedClient(key, 'multi');
  return client.multi(multiArr);
};

ShardedRedisClient.prototype.zaddMulti = function (key, arr, cb) {
  var client = this._findMatchedClient(key, 'zadd');
  cb = once(cb);
  var timeout;
  if (this._writeTimeout) timeout = setTimeout(cb, this._writeTimeout, new Error('Redis call timed out'));
  return client.zadd([key].concat(arr), wrappedCb);

  function wrappedCb(err, results) {
    clearTimeout(timeout);
    cb(err, results);
  }
};

ShardedRedisClient.prototype.zremMulti = function (key, arr, cb) {
  var client = this._findMatchedClient(key, 'zrem');
  cb = once(cb);
  var timeout;
  if (this._writeTimeout) timeout = setTimeout(cb, this._writeTimeout, new Error('Redis call timed out'));
  return client.zrem([key].concat(arr), wrappedCb);

  function wrappedCb(err, results) {
    clearTimeout(timeout);
    cb(err, results);
  }
};

// TODO: fix this to new logic
ShardedRedisClient.prototype.keys = function (pattern, done) {
  var _this = this;
  var allKeys = {};
  var readClients = _this._wrappedClients.map(function (c, i) { return _this._getReadClient(i); });

  async.each(readClients, function (rc, cb) {
    rc.keys(pattern, function (err, keys) {
      if (err) return cb(err);
      keys.forEach(function (k) { allKeys[k] = true; });

      cb();
    });
  }, function (err) {

    if (err) return done(err);
    done(null, Object.keys(allKeys));
  });

};

function WrappedClient(conf, options) {
  var _this = this;

  var client = createClient(conf.port, conf.host, options);

  var slaveClients = conf.slaves.map(function (slaveHost) {
    return createClient(conf.port, slaveHost, options);
  });

  if (!slaveClients.length) {
    slaveClients.push(client);
  }

  client._isMaster = true;
  _this.client = client;
  _this.slaves = new RoundRobinSet(slaveClients);
  _this.readPreference = conf.readPreference;

}

WrappedClient.prototype.get = function () {
  return this.client;
};

WrappedClient.prototype.getSlave = function () {
  return this.slaves.obtain();
};

function HostRange(host, startPort, endPort, slaveHosts, readPreference) {

  var _this = this;
  _this.host = host;
  _this.startPort = startPort;
  _this.endPort = endPort || startPort;
  _this.slaveHosts = slaveHosts || [];
  _this.readPreference = readPreference;
}

HostRange.prototype.toArray = function () {

  var _this = this;
  var set = [];

  for (var i = _this.startPort; i <= _this.endPort; i++) {
    set.push({ host: _this.host, port: i, slaves: _this.slaveHosts, readPreference: _this.readPreference });
  }

  return set;

};

function ShardSet(hostRanges) {

  var _this = this;
  _this.hostRanges = hostRanges;

}

ShardSet.prototype.toArray = function () {

  var _this = this;

  return _this.hostRanges.reduce(function (memo, hostRange) {
    return memo.concat(hostRange.toArray());
  }, []);

};

function RoundRobinSet(arr) {
  this._current = 0;
  var newArr = arr.slice(0);
  for (var i = 0, l = newArr.length; i < l; i++) {
    newArr[i]._rrindex = i;
  }

  Object.defineProperty(this, 'items', { value: newArr });
}

RoundRobinSet.prototype.obtain = function () {
  var item = this.items[this._current];
  this._current = (this._current + 1) % this.items.length;
  return item;
};

RoundRobinSet.prototype.next = function (item) {
  return this.items[(item._rrindex + 1) % this.items.length];
};

function getNode(key, shards) {
  var hash = crypto.createHash('sha1').update(String(key)).digest('hex');
  var hashCut = hash.substring(0, 4);
  var hashNum = parseInt(hashCut, 16);
  return hashNum % shards;
}

function createClient(port, host, options) {
  var client = redis.createClient(port, host, options.redisOptions);

  if (options.usePing) {
    setTimeout(function () {
      setInterval(function () {
        client.ping(noop);
      }, 150 * 1000);
    }, Math.floor(Math.random() * (150 * 1000 + 1)));
  }

  if (options.breakerConfig) {
    client._breaker = new CircuitBreaker(options.breakerConfig);
  }

  client.on('error', function (e) {
    console.log('Redis Error [' + host + ':' + port + ']: ' + e + ' : ' + (new Date()).toISOString());
  });

  client.on('end', function (e) {
    console.log('Redis End [' + host + ':' + port + ']: ' + e + ' : ' + (new Date()).toISOString());
  });

  return client;
}

function once(func) {
  var called = false;
  return function () {
    if (called) return;
    called = true;
    return func.apply(this, arguments);
  };
}

function noop() {}
