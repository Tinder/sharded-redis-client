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

const proxyquire = require('proxyquire');
const MockRedisClient = require('./MockRedisClient');

describe('Test breakers', () => {

  const WrappedClient = proxyquire('../lib/WrappedClient', { redis: MockRedisClient });
  const ShardedRedis = proxyquire('../ShardedRedisClient', { './lib/WrappedClient': WrappedClient });

  it('should call the appropriate redis function with a closed breaker', (done) => {
    const key = 'key';
    const numMasterHosts = 1;
    const redisHosts = global.generateRedisHosts(numMasterHosts);
    const options = {
      breakerConfig: {
        threshold: 0.5,
        circuitDuration: 15000,
        timeout: 5000
      }
    };

    const shardedClient = new ShardedRedis(redisHosts, options);

    spyOn(MockRedisClient.prototype, 'get').and.callThrough();

    shardedClient.get(key, (err) => {
      expect(err).toBe(null);
      expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1);
      done();
    });
  });

  it('should receive error when redis command fails', function (done) {
    const key = 'key';
    const numMasterHosts = 1;
    const redisHosts = global.generateRedisHosts(numMasterHosts);
    const options = {
      breakerConfig: {
        threshold: 0.5,
        circuitDuration: 15000,
        timeout: 5000
      }
    };

    const shardedClient = new ShardedRedis(redisHosts, options);

    spyOn(MockRedisClient.prototype, 'get').and.callFake((key, cb) => cb(new Error('an error from the redis client')));

    shardedClient.get(key, (err) => {
      expect(err instanceof Error).toBeTrue();
      expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1);
      done();
    });
  });

  it('should not call the redis function if the breaker is open', function (done) {
    const key = 'key';
    const numMasterHosts = 1;
    const redisHosts = global.generateRedisHosts(numMasterHosts);
    const options = {
      breakerConfig: {
        threshold: 0.5,
        circuitDuration: 15000,
        timeout: 5000
      }
    };

    const shardedClient = new ShardedRedis(redisHosts, options);
    const mockedClient = shardedClient._getWrappedClient(key).get();
    
    // manually open it first
    mockedClient._breaker._open();
    spyOn(MockRedisClient.prototype, 'get').and.callThrough();

    shardedClient.get(key, (err) => {
      expect(err instanceof Error).toBeTrue();
      expect(MockRedisClient.prototype.get).not.toHaveBeenCalled();
      done();
    });
  });
});
