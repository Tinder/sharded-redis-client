describe('Sharded Client tests', function () {
  var async = require('async');

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
  var MockRedisClient = require('../mocks/mockRedisClient.js');
  var proxyquire = require('proxyquire');

  // Reset before each test
  var ShardedRedis;

  /********************************
   * Setup
   ********************************/

  beforeEach(function () {
    // Get sharded-redis-client with the redis clients stubbed out
    ShardedRedis = proxyquire('../../sharded-redis-client.js', {
      redis: MockRedisClient
    });
  });

  /********************************
   * Tests
   ********************************/

  it('should create a node_redis client when created', function () {
    spyOn(MockRedisClient, 'createClient').and.callThrough();
    var numMasterHosts = 1;
    var options = { redisOptions: {} };
    var redisHosts = generateRedisHosts(numMasterHosts);

    var shardedClient = new ShardedRedis(redisHosts, options);
    expect(MockRedisClient.createClient).toHaveBeenCalledTimes(1);
    expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[0].port_range[0], redisHosts[0].host, options.redisOptions);
  });

  it('should create master and slave clients', function () {
    spyOn(MockRedisClient, 'createClient').and.callThrough();

    var numMasterHosts = 1;
    var numPorts = 1;
    var numSlaves = 3;
    var redisHosts = generateRedisHosts(numMasterHosts, numPorts, numSlaves);
    var shardedClient = new ShardedRedis(redisHosts);

    expect(MockRedisClient.createClient).toHaveBeenCalledTimes(numSlaves + 1);
    expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[0].port_range[0], redisHosts[0].host, undefined);
    for (var i = 0; i < numSlaves; i++) {
      expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[0].port_range[0], redisHosts[0].slaveHosts[i], undefined);
    }
  });

  it('should create multiple master clients', function () {
    spyOn(MockRedisClient, 'createClient').and.callThrough();

    var numMasterHosts = 3;
    var numPorts = 5;
    var redisHosts = generateRedisHosts(numMasterHosts, numPorts);
    var shardedClient = new ShardedRedis(redisHosts);

    expect(MockRedisClient.createClient).toHaveBeenCalledTimes(numMasterHosts * numPorts);
    for (var i = 0; i < numMasterHosts; i++) {
      for (var j = 0; j < numPorts; j++) {
        expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[i].port_range[0] + j, redisHosts[i].host, undefined);
      }
    }
  });

  it('should call the corresponding redis command on the redis client', function (done) {
    shardable.forEach(function (cmd) {
      spyOn(MockRedisClient.prototype, cmd).and.callThrough();
    });

    var numMasterHosts = 1;
    var shardedClient = new ShardedRedis(generateRedisHosts(numMasterHosts));

    async.each(shardable, function (cmd, cb) {
      shardedClient[cmd]('key', function () {
        expect(MockRedisClient.prototype[cmd]).toHaveBeenCalledTimes(1);
        cb();
      });
    }, done);
  });

  it('should go to the same wrapped client when multiple requests are made with the same key', function (done) {
    var numMasterHosts = 3;
    var numPorts = 5;
    var callTimes = 5;
    var redisHosts = [];
    var key = 'key';

    var shardedClient = new ShardedRedis(generateRedisHosts(numMasterHosts, numPorts));
    var mockedRedisClient = shardedClient._getWrappedClient(key).get();
    spyOn(mockedRedisClient, 'get').and.callThrough();

    async.times(callTimes, function (n, cb) {
      shardedClient.get(key, cb);
    },

      function () {
      expect(mockedRedisClient.get).toHaveBeenCalledTimes(callTimes);
      done();
    });
  });

  it('should not always go to the same redis client', function (done) {
    var numMasterHosts = 3;
    var numPorts = 3;
    var numKeys = 100;
    var currKeyNum = 0;

    var shardedClient = new ShardedRedis(generateRedisHosts(numMasterHosts, numPorts));

    var wrappedClients = shardedClient._wrappedClients;
    for (var i = 0; i < numMasterHosts * numPorts; i++) {
      spyOn(wrappedClients[i].get(), 'get').and.callThrough();
    }

    async.times(numKeys, function (n, cb) {
        var key = 'key' + currKeyNum++;
        shardedClient.get(key, cb);
      },

      function () {
        var sumCalls = 0;
        for (i = 0; i < numMasterHosts * numPorts; i++) {
          sumCalls += wrappedClients[i].get().get.calls.count();
          expect(wrappedClients[i].get().get.calls.count()).toBeLessThan(numKeys);
        }

        expect(sumCalls).toBe(numKeys);
        done();
      });
  });

  it('should try to read from slaves first when readPreference is "slave"', function (done) {
    var key = 'key';
    var numMasterHosts = 1;
    var numPorts = 1;
    var numSlaveHosts = 5;
    var totalCalls = 0;
    var masterCallNumber;
    var shardedClient = new ShardedRedis(generateRedisHosts(numMasterHosts, numPorts, numSlaveHosts));
    shardedClient.on('error', noop);

    var wrappedClient = shardedClient._getWrappedClient(key);
    var masterClient = wrappedClient.get();

    var slaveClients = [];
    for (var i = 0; i < numSlaveHosts; i++) {
      var sc = wrappedClient.getSlave();
      slaveClients.push(sc);
      spyOn(sc, 'get').and.callFake(function (key, cb) {
        totalCalls++;
        cb(new Error('an error'));
      });
    }

    spyOn(masterClient, 'get').and.callFake(function (key, cb) {
      totalCalls++;
      masterCallNumber = totalCalls;
      cb();
    });

    shardedClient.get(key, function () {
      slaveClients.forEach(function (sc) {
        expect(sc.get).toHaveBeenCalledTimes(1);
        expect(sc.get.calls.argsFor(0).slice(0, -1)).toEqual([key]);
      });

      expect(masterClient.get).toHaveBeenCalledTimes(1);
      expect(masterClient.get.calls.argsFor(0).slice(0, -1)).toEqual([key]);
      expect(masterCallNumber).toEqual(totalCalls);
      done();
    });
  });

  it('should only make write calls to master, even if readPreference is "slave"', function (done) {
    var key = 'key';
    var value = 'value';
    var numMasterHosts = 1;
    var numPorts = 1;
    var numSlaveHosts = 5;
    var shardedClient = new ShardedRedis(generateRedisHosts(numMasterHosts, numPorts, numSlaveHosts));
    shardedClient.on('error', noop);

    var wrappedClient = shardedClient._getWrappedClient(key);
    var masterClient = wrappedClient.get();

    var slaveClients = [];
    for (var i = 0; i < numSlaveHosts; i++) {
      var sc = wrappedClient.getSlave();
      slaveClients.push(sc);
      spyOn(sc, 'set').and.callThrough();
    }

    spyOn(masterClient, 'set').and.callThrough();

    shardedClient.set(key, value, function () {
      slaveClients.forEach(function (sc) {
        expect(sc.set).not.toHaveBeenCalled();
      });

      expect(masterClient.set).toHaveBeenCalledTimes(1);
      expect(masterClient.set.calls.argsFor(0).slice(0, -1)).toEqual([key, value]);
      done();
    });
  });
});

function generateRedisHosts(numMasters, numPorts, numSlaves) {
  var hosts = [];
  var counter = 0;
  for (var m = 0; m < numMasters; m++) {
    var masterConfig = generateRedisConfig(numPorts);
    for (var s = 0; s < numSlaves; s++) {
      if (!masterConfig.slaveHosts) masterConfig.slaveHosts = [];
      masterConfig.slaveHosts.push(generateRedisConfig().host);
      masterConfig.readPreference = 'slave';
    }

    hosts.push(masterConfig);
  }

  return hosts;

  function generateRedisConfig(numPorts) {
    var portRange = [6379];
    if (numPorts > 1) portRange.push(6379 + numPorts - 1);

    return {
      host: 'http://hostname' + counter++,
      port_range: portRange
    };
  }
}