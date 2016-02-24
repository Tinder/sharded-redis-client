/**
 * sharded-redis-client/tests/tests.js
 * @description: ShardedRedisClient unit tests for slave retry
 * @author: Chris Young (chris.young@gotinder.com)
 */

var assert = require('assert'),
    async = require('async'),
    sinon = require('sinon');

var StubbedRedisClient = require(__dirname + '/StubbedRedisClient.js'),
    StubbedWrappedClient = require(__dirname + '/StubbedWrappedClient.js'),
    ShardedRedisClient = require(__dirname + '/../sharded-redis-client.js');

/**
 * log_client()
 * @description: Displays ShardedRedisClient master and slave configuration for debugging
 * @param: {ShardedRedisClient} client
 */
function log_client(client) {
  var clients = client._wrappedClients.map(function(wrappedClient, index) {
    var slaves = wrappedClient.slaves.items.map(function(slave) {
      return slave._rrindex;
    });
    return '\t{ wrappedClient: ' + index + '\tslaveHosts: [' + slaves.join(', ') + '] }';
  });
  console.log('configuration = [\n' + clients.join(',\n') + '\n]');
}

/**
 * log_attempts()
 * @description: Displays StubbedRedisClient retries for debugging
 * @param: {Array} spies
 */
function log_attempts(spies) {
  var sp = spies.map(function(spy, index) {
    var s = spy.slaves.map(function(slave) {
      return 'slaveHost: ' + slave.get.callCount;
    });
    return '\t{ wrappedClient: ' + spy.get_client.callCount + '\t' + s.join(', ') + ' }';
  });
  console.log('attempts = [\n' + sp.join(',\n') + '\n]');
}

describe('stubbed_redis_client', function() {
  var stubbed_redis_client = new StubbedRedisClient(),
      test_data = 'foo';

  before(function() {
    sinon.spy(stubbed_redis_client, 'set');
    sinon.spy(stubbed_redis_client, 'get');
  });

  it('Can persist data', function(done) {
    stubbed_redis_client.set('test_data', test_data, function(error, result) {
      assert.equal('OK', result);
      done();
    });
  });

  it('Can retrieve data', function(done) {
    stubbed_redis_client.get('test_data', function(error, result) {
      assert.equal(test_data, result);
      done();
    });
  });

  it('Spies on get and set', function() {
    assert(stubbed_redis_client.set.calledWith('test_data'));
    assert(stubbed_redis_client.get.calledWith('test_data'));
  });
});

describe('stubbed_wrapped_client', function() {
  var stubbed_redis_client = new StubbedRedisClient(),
      stubbed_wrapped_client = new StubbedWrappedClient(stubbed_redis_client);

  sinon.spy(stubbed_wrapped_client.slaves, 'obtain');
  sinon.spy(stubbed_wrapped_client.slaves, 'next');

  var slave = stubbed_wrapped_client.slaves.obtain();

  it('Returns slaves from its round robin set', function() {
    assert.equal(stubbed_redis_client, slave);
    assert.equal(stubbed_redis_client, stubbed_wrapped_client.slaves.next(slave));
  });

  it('Spies on its round robin set', function() {
    assert(stubbed_wrapped_client.slaves.obtain.calledOnce);
    assert(stubbed_wrapped_client.slaves.next.calledWith(slave));
  });
});

describe('stubbed_sharded_redis_client', function() {
  describe('can be used without "slaveHosts"', function() {
    var stubbed_redis_client = new StubbedRedisClient(),
        stubbed_wrapped_client = new StubbedWrappedClient(stubbed_redis_client),
        stubbed_sharded_redis_client = new ShardedRedisClient([]);

    var test_data = 'bar';

    before(function() {
      stubbed_sharded_redis_client._wrappedClients.push(stubbed_wrapped_client);
      sinon.spy(stubbed_redis_client, 'get');
      sinon.spy(stubbed_redis_client, 'set');
      sinon.spy(stubbed_wrapped_client, 'get');
    });

    it('Simulates a set call', function(done) {
      stubbed_sharded_redis_client.set('test_data', test_data, function(error, results) {
        assert.equal(null, error);
        assert.equal('OK', results);
        done();
      });
    });

    it('Simulates a get call', function(done) {
      stubbed_sharded_redis_client.get('test_data', function(error, results) {
        assert.equal(null, error);
        assert.equal(test_data, results);
        done();
      });
    });

    it('Spies on its slaves', function() {
      assert(stubbed_sharded_redis_client._wrappedClients[0].get.called);
      assert(stubbed_sharded_redis_client._wrappedClients[0].client.set.called);
      assert(stubbed_sharded_redis_client._wrappedClients[0].client.get.called);
    });
  });

  describe('can be used with "slaveHosts"', function() {
    var multi_client = new ShardedRedisClient([]),
        spies = [];

    var test_data = 'baz',
        failures = 2;

    before(function() {
      for (var x = 0; x < 4; x++) {
        var stubbed_redis_client = new StubbedRedisClient();
        var slave_clients = [0, 1, 2, 3].map(function(y) {
          return new StubbedRedisClient(x == 0 && y < failures);
        });

        var stubbed_wrapped_client = new StubbedWrappedClient(stubbed_redis_client, slave_clients);
        multi_client._wrappedClients.push(stubbed_wrapped_client);

        spies.push({
          set: sinon.spy(stubbed_redis_client, 'set'),
          get: sinon.spy(stubbed_redis_client, 'get'),
          get_client: sinon.spy(stubbed_wrapped_client, 'get'),
          obtain: sinon.spy(stubbed_wrapped_client.slaves, 'obtain'),
          next: sinon.spy(stubbed_wrapped_client.slaves, 'next'),
          slaves: slave_clients.map(function(s) {
            return {
              set: sinon.spy(s, 'set'),
              get: sinon.spy(s, 'get'),
              client: s
            };
          })
        });
      }

      // log_client(multi_client);
    });

    it('Can persist data', function(done) {
      multi_client.set('test_data', test_data, function(error, result) {
        assert.equal(null, error);
        assert.equal('OK', result);
        done();
      });
    });

    it('Can retrieve data', function(done) {
      multi_client.get('test_data', function(error, result) {
        assert.equal(null, error);
        assert.equal(test_data, result);
        done();
      });
    });

    it('Should retry other slaves if one fails', function() {
      spies.forEach(function(spy) {
        spy.slaves.forEach(function(slave) {
          if (slave.get.called)
            assert.equal(failures + 1, slave.get.callCount);
        });
      });

      // log_attempts(spies);
    });
  });
});
