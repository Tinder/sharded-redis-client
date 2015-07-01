var redis = require('redis');
var crypto = require('crypto');
var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var async = require('async');

module.exports = ShardedRedisClient ;

/**
 * Creates a dummy redis client that consistently shards by key using sha1
 * @param {configurationArray} array - list of "HostRange" configuration objects
 *
 * ex 1:
 *  new ShardedRedisClient([
 *    { 'host' : 'box1.redis' , port_range : [6370, 6372] },
 *    { 'host' : 'box2.redis' , port_range : [10000] },
 *    { 'host' : 'box3.redis' , port_range : [6379, 6380], slaveHosts:["box3.redis.slave1","box3.redis.slave2"], readPreference: "slave" }, // <- force read slave on this set
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

function ShardedRedisClient( configurationArray, use_ping, logger ){

  if (use_ping !== false) use_ping = true;

  assert( Array.isArray( configurationArray ) , 'first argument \'configurationArray\' must be an array.' );

  var self = this;
  var hostRanges = configurationArray.map(function(hostRangeConfig){
    return new HostRange( hostRangeConfig.host , hostRangeConfig.port_range[0] , hostRangeConfig.port_range[1], hostRangeConfig.slaveHosts, hostRangeConfig.readPreference )
  });
  var shardSet = new ShardSet(hostRanges);
  var wrappedClients = shardSet.toArray().map(function(conf){
    return new WrappedClient(conf, use_ping);
  });

  Object.defineProperty(self, "_usePing", { value: use_ping });
  Object.defineProperty(self, "_readSlave", { value : false });
  Object.defineProperty(self, "_wrappedClients", { value : wrappedClients});
  Object.defineProperty(self, "_ringSize", { value : wrappedClients.length})
  Object.defineProperty(self, "_logger", { value : logger });
}

ShardedRedisClient.prototype.slaveOk = function () {
  var self = this ;
  return Object.create(self,{
    _readSlave: {value:true}
  });
};

ShardedRedisClient.prototype._findMatchedClient = function (key, cmd) {
  key = key.toString();
  var clientIndex = this._getClientIndex(key);
  var isReadCmd = readOnly.indexOf(cmd) >= 0 ;
  return isReadCmd ? this._getReadClient(clientIndex) : this._getWriteClient(clientIndex);
};

ShardedRedisClient.prototype._findMasterClient = function (key) {
  key = key.toString();
  var clientIndex = this._getClientIndex(key);
  var wrappedClient = this._wrappedClients[ clientIndex ];
  return wrappedClient.get();
};

ShardedRedisClient.prototype._getReadClient = function (clientIndex) {
  var wrappedClient = this._wrappedClients[ clientIndex ];
  var slaveOk = this._readSlave || wrappedClient.readPreference == "slave" ;
  return slaveOk ? wrappedClient.getSlave() : wrappedClient.get();
};

ShardedRedisClient.prototype._getWriteClient = function (clientIndex) {
  var wrappedClient = this._wrappedClients[ clientIndex ];
  return wrappedClient.get();
};

ShardedRedisClient.prototype._getWrappedClient = function (key) {
  var clientIndex = this._getClientIndex(key);
  return this._wrappedClients[ clientIndex ];
};

ShardedRedisClient.prototype._getClientIndex = function (key) {
  return getNode(key, this._wrappedClients.length);
};

shardable.forEach(function(cmd){

  // TODO: check that this works
  ShardedRedisClient.prototype[cmd] = function ( /* arguments */ ) {
    var self = this;
    var args = arguments;
    var key = Array.isArray(arguments[0]) ? arguments[0][0] : arguments[0];

    var client = this._findMatchedClient(key, cmd);

    var startIndex = client._rrindex;
    var commandFn = client[cmd];
    var wrappedClient = self._getWrappedClient(key);
    var logger = self._logger;

    var mainCb = args[args.length - 1];
    if(typeof mainCb !== "function") mainCb = args[args.length] = noop;

    args[args.length - 1] = function (err) {
        if (logger && logger.warn) {
          logger.warn(new Date().toISOString(), 'sharded-redis-client [' + client.address + '] err: ' + err);
        } else {
          console.error(new Date().toISOString(), 'sharded-redis-client [' + client.address + '] err: ' + err);
        }
      }

      if (err && !client._isMaster) {
        client = wrappedClient.slaves.next(client);
        if (client._rrindex == startIndex) {
          client = self._findMasterClient(key);
        }
        commandFn.apply(client, args);
      }
      mainCb.apply(this, arguments);
    };
    commandFn.apply(client, args);
  };

});

/* Intentionally opinionated implementations of a few methods: */

ShardedRedisClient.prototype.multi = function ( key , multiArr ) {
  var client = this._findMatchedClient(key,'multi');
  return client.multi(multiArr);
};

ShardedRedisClient.prototype.zaddMulti = function ( key , arr, cb ) {
  var client = this._findMatchedClient(key,'zadd');
  return client.zadd([key].concat(arr), cb);
};

ShardedRedisClient.prototype.zremMulti = function ( key , arr, cb ) {
  var client = this._findMatchedClient(key,'zrem');
  return client.zrem([key].concat(arr), cb);
};

// TODO: fix this to new logic
ShardedRedisClient.prototype.keys = function ( pattern , done ) {
  var self = this ;
  var allKeys = {};
  var readClients = self._wrappedClients.map(function(c,i){ return self._getReadClient(i) });

  async.each(readClients, function(rc, cb) {
    rc.keys(pattern, function(err, keys) {
      if (err) return cb(err);
      keys.forEach(function (k) { allKeys[k] = true; });
      cb();
    });
  },function(err){
    if (err) return done(err);
    done(null,Object.keys(allKeys))
  });

};

function WrappedClient (conf, use_ping) {
  var self = this ;

  var client = create_client(conf.port, conf.host, use_ping);

  var slaveClients = conf.slaves.map(function(slaveHost){
    return create_client(conf.port, slaveHost, use_ping);
  });

  if (!slaveClients.length) {
    slaveClients.push(client);
  }
  client._isMaster = true;
  self.client = client;
  self.slaves = new RoundRobinSet(slaveClients);
  self.readPreference = conf.readPreference;

}

WrappedClient.prototype.get = function () {
  return this.client;
};

WrappedClient.prototype.getSlave = function () {
  return this.slaves.obtain()
};

function HostRange ( host , startPort , endPort, slaveHosts, readPreference ) {

  var self = this ;
  self.host = host ;
  self.startPort = startPort ;
  self.endPort = endPort || startPort ;
  self.slaveHosts = slaveHosts || [] ;
  self.readPreference = readPreference ;
}

HostRange.prototype.toArray = function () {

  var self = this ;
  var set = [] ;

  for ( var i = self.startPort ; i <= self.endPort ; i++  ) {
    set.push({ host : self.host , port : i , slaves: self.slaveHosts , readPreference: self.readPreference  })
  }

  return set;

};

function ShardSet ( hostRanges ) {

  var self = this ;
  self.hostRanges = hostRanges ;

}

ShardSet.prototype.toArray = function () {

  var self = this ;

  return self.hostRanges.reduce(function(memo,hostRange){
    return memo.concat(hostRange.toArray());
  },[]);

};

function RoundRobinSet ( arr ) {
  this._current = 0;
  var newArr = arr.slice(0);
  for (var i = 0, l = newArr.length; i < l; i++) {
    newArr[i]._rrindex = i;
  }
  Object.defineProperty(this,"items",{ value: newArr });
}

RoundRobinSet.prototype.obtain = function (){
  var item = this.items[this._current];
  this._current = (this._current + 1) % this.items.length;
  return item;
};

RoundRobinSet.prototype.next = function (item){
  return this.items[(item._rrindex + 1) % this.items.length];
};

function getNode(key, shards){
  var hash = crypto.createHash('sha1').update(String(key)).digest("hex");
  var hashCut = hash.substring(0,4);
  var hashNum = parseInt(hashCut, 16);
  return hashNum%shards;
}

function create_client(port, host, use_ping) {
  var client = redis.createClient( port , host );

  if (use_ping) {
    setTimeout(function() {
      setInterval(function() {
        client.ping(noop);
      }, 150 * 1000);
    }, Math.floor(Math.random() * (150 * 1000 + 1)));
  }

  client.on("error",function(e){
    console.log('Redis Error [' + host + ':' + port + ']: ' + e + ' : ' + (new Date()).toISOString());
  });

  client.on('end', function(e) {
    console.log('Redis End [' + host + ':' + port + ']: ' + e + ' : ' + (new Date()).toISOString())
  });

  return client;
}

function noop() {}
