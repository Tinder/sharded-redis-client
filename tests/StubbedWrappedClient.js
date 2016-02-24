var RoundRobinSet = require(__dirname + '/RoundRobinSet.js');

/**
 * StubbedWrappedClient()
 * @description: Mocks the WrappedClient class for unit testing
 * @param: {StubbedRedisClient} stubbed_redis_client
 * @param: {StubbedRedisClients Array} slave_clients
 */
function StubbedWrappedClient(stubbed_redis_client, slave_clients) {
  var that = this;

  this.client = stubbed_redis_client;
  this.client._isMaster = true;

  if (!slave_clients) {
    slave_clients = [stubbed_redis_client];
  } else {
    var fn = this.client.set;
    this.client.set = function(key, value, callback) {
      that.slaves.items.forEach(function(slave) {
        slave.set(key, value, function() { return 0; });
      });
      fn.apply(that.client, arguments);
    };
  }

  this.slaves = new RoundRobinSet(slave_clients);
  this.readPreference = 'slave';
}

/**
 * StubbedWrappedClient.get()
 * @description: Returns the master client
 * @returns: {StubbedRedisClient}
 */
StubbedWrappedClient.prototype.get = function() {
  return this.client;
};

/**
 * StubbedWrappedClient.getSlave()
 * @description: Iterates over the WrappedClients RoundRobinSet
 * @returns: {StubbedRedisClient}
 */
StubbedWrappedClient.prototype.getSlave = function() {
  return this.slaves.obtain();
};

module.exports = StubbedWrappedClient;
