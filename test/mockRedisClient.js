/**
 * Mocks the necessary pieces of the node redis client
 * @constructor
 */
var EventEmitter = require('events').EventEmitter;

function MockRedisClient(port, host) {
  this.port = port;
  this.host = host;
  this.address = host + ':' + port;
}

MockRedisClient.prototype = {
  __proto__: EventEmitter.prototype,
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
  _ready: function () { this.emit('ready'); }
};

module.exports = MockRedisClient;
module.exports.createClient = function (port, host, options) {
  return new MockRedisClient(port, host, options);
};