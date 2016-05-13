describe('Test breakers', function () {
  var MockRedisClient = require('./mockRedisClient.js');
  var proxyquire = require('proxyquire');

  // Reset before each test
  var ShardedRedis;

  /********************************
   * Setup
   ********************************/

  beforeEach(function () {
    // Get sharded-redis-client with the redis clients stubbed out
    ShardedRedis = proxyquire('../sharded-redis-client.js', {
      redis: MockRedisClient
    });
  });

  /********************************
   * Tests
   ********************************/

  it('should call the appropriate redis function with a closed breaker', function (done) {
    spyOn(MockRedisClient.prototype, 'get').and.callThrough();
    var numMasterHosts = 1;
    var options = {
      breakerConfig: {
        failure_rate: 0.5,
        failure_count: 10,
        reset_timeout: 30000
      }
    };
    var key = 'key';
    var redisHosts = generateRedisHosts(numMasterHosts);

    var shardedClient = new ShardedRedis(redisHosts, options);
    var mockedClient = shardedClient._getWrappedClient(key).get();
    spyOn(mockedClient._breaker, 'pass').and.callThrough();

    shardedClient.get(key, function (err) {
      expect(err).toBeUndefined();
      expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1);
      expect(mockedClient._breaker.pass).toHaveBeenCalledTimes(1);
      done();
    });
  });

  it('should tell the breaker about a failed redis command', function (done) {
    spyOn(MockRedisClient.prototype, 'get').and.callFake(function (key, cb) {
      cb(new Error('an error from the redis client'));
    });

    var numMasterHosts = 1;
    var options = {
      breakerConfig: {
        failure_rate: 0.5,
        failure_count: 10,
        reset_timeout: 30000
      }
    };
    var key = 'key';
    var redisHosts = generateRedisHosts(numMasterHosts);

    var shardedClient = new ShardedRedis(redisHosts, options);
    var mockedClient = shardedClient._getWrappedClient(key).get();
    spyOn(mockedClient._breaker, 'fail').and.callThrough();

    shardedClient.get(key, function (err) {
      expect(err).toBeTruthy();
      expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1);
      expect(mockedClient._breaker.fail).toHaveBeenCalledTimes(1);
      done();
    });
  });

  it('should not call the redis function if the breaker is open', function (done) {
    spyOn(MockRedisClient.prototype, 'get').and.callThrough();
    var numMasterHosts = 1;
    var options = {
      // this config ensures a perpetually open breaker
      breakerConfig: {
        failure_count: -1
      }
    };
    var key = 'key';
    var redisHosts = generateRedisHosts(numMasterHosts);

    var shardedClient = new ShardedRedis(redisHosts, options);

    var mockedClient = shardedClient._getWrappedClient(key).get();
    mockedClient._breaker.trip();

    shardedClient.get(key, function (err) {
      expect(err).toBeTruthy();
      expect(err.message).toBe('breaker open');
      expect(MockRedisClient.prototype.get).not.toHaveBeenCalled();
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