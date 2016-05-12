describe('Test timeouts', function () {
  var async = require('async');
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
    jasmine.clock().install();
  });

  afterEach(function () {
    jasmine.clock().uninstall();
  });

  /********************************
   * Tests
   ********************************/

  it('should cb with an error on a read if redis is not responding if read timeouts enabled', function (done) {
    spyOn(MockRedisClient.prototype, 'get');
    var numMasterHosts = 1;
    var options = {
      readTimeout: 500
    };
    var key = 'key';
    var redisHosts = generateRedisHosts(numMasterHosts);

    var shardedClient = new ShardedRedis(redisHosts, options);

    // ignore the fact that we emitted an error
    shardedClient.on('error', noop);

    shardedClient.get(key, function (err) {
      expect(err).toBeTruthy();
      expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1);
      expect(err.message).toBe('Redis call timed out');
      done();
    });

    jasmine.clock().tick(500);
  });

  it('should cb with an error on a write if redis is not responding if write timeouts enabled', function (done) {
    spyOn(MockRedisClient.prototype, 'set');
    var numMasterHosts = 1;
    var options = {
      writeTimeout: 1000
    };
    var key = 'key';
    var value = 'value';
    var redisHosts = generateRedisHosts(numMasterHosts);

    var shardedClient = new ShardedRedis(redisHosts, options);

    // ignore the fact that we emitted an error
    shardedClient.on('error', noop);

    shardedClient.set(key, value, function (err) {
      expect(err).toBeTruthy();
      expect(MockRedisClient.prototype.set).toHaveBeenCalledTimes(1);
      expect(err.message).toBe('Redis call timed out');
      done();
    });

    jasmine.clock().tick(1000);
  });

  it('should only call the cb once if a timeout is not triggered', function (done) {
    spyOn(MockRedisClient.prototype, 'get').and.callThrough();
    var numMasterHosts = 1;
    var options = {
      readTimeout: 500
    };
    var key = 'key';
    var redisHosts = generateRedisHosts(numMasterHosts);

    var shardedClient = new ShardedRedis(redisHosts, options);

    // ignore the fact that we emitted an error
    shardedClient.on('error', noop);

    var mainCbSpy = jasmine.createSpy('mainCb').and.callFake(function (err) {
      expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1);
    });

    shardedClient.get(key, mainCbSpy);

    setTimeout(function () {
      expect(mainCbSpy).toHaveBeenCalledTimes(1);
      done();
    }, 600);

    jasmine.clock().tick(600);
  });

  it('should move on to the next client if redis client times out on reads', function (done) {
    var numMasterHosts = 1;
    var numPorts = 1;
    var numSlaves = 2;
    var options = {
      readTimeout: 500
    };
    var key = 'key';
    var redisHosts = generateRedisHosts(numMasterHosts, numPorts, numSlaves);

    var shardedClient = new ShardedRedis(redisHosts, options);

    var wrappedClient = shardedClient._getWrappedClient(key);
    var masterClient = wrappedClient.get();
    var slaveClient1 = wrappedClient.getSlave();
    var slaveClient2 = wrappedClient.getSlave();
    spyOn(masterClient, 'get').and.callThrough();
    spyOn(slaveClient1, 'get');
    spyOn(slaveClient2, 'get');

    // ignore the fact that we emitted an error
    shardedClient.on('error', noop);

    shardedClient.get(key, function (err) {
      expect(err).toBeUndefined();
      expect(slaveClient1.get).toHaveBeenCalledTimes(1);
      expect(slaveClient2.get).toHaveBeenCalledTimes(1);
      expect(masterClient.get).toHaveBeenCalledTimes(1);
      done();
    });

    jasmine.clock().tick(1000);
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