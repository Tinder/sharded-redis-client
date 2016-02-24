/**
 * StubbedRedisClient()
 * @description: Mock Redis client class for unit testing
 * @param: {Boolean} fail
 */
function StubbedRedisClient(fail) {
  this.data = {};
  this.fail = fail;
}

/**
 * StubbedRedisClient.set()
 * @description: Persists data to the mock client
 * @param: {String} key
 * @param: {String} value
 * @param: {Function} callback
 */
StubbedRedisClient.prototype.set = function(key, value, callback) {
  if (this.fail) {
    callback('error', null);
  } else {
    this.data[key] = value;
    callback(null, 'OK');
  }
};

/**
 * StubbedRedisClient.get()
 * @description: Retrieves data from the mock server
 * @param: {String} key
 * @param: {Function} callback
 */
StubbedRedisClient.prototype.get = function(key, callback) {
  if (this.fail)
    callback('error', null);
  else
    callback(null, this.data[key]);
};

module.exports = StubbedRedisClient;
