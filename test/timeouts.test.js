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

const async = require('async');
const proxyquire = require('proxyquire');
const MockRedisClient = require('./MockRedisClient');

describe('Test timeouts', function () {

  const WrappedClient = proxyquire('../lib/WrappedClient', { redis: MockRedisClient });
  const ShardedRedis = proxyquire('../ShardedRedisClient', { './lib/WrappedClient': WrappedClient });

  beforeAll(() => jasmine.clock().install());
  afterAll(() => jasmine.clock().uninstall());

  it('should cb with an error on a read if redis is not responding if read timeouts enabled', function (done) {
    const key = 'key';
    const numMasterHosts = 1;
    const options = { readTimeout: 500 };
    const redisHosts = global.generateRedisHosts(numMasterHosts);
    const shardedClient = new ShardedRedis(redisHosts, options);

    spyOn(MockRedisClient.prototype, 'get');

    shardedClient.get(key, (err) => {
      expect(err instanceof Error).toBeTrue();
      expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1);
      expect(err.message).toBe('Redis call timed out');
      done();
    });

    jasmine.clock().tick(500);
  });

  it('should cb with an error on a write if redis is not responding if write timeouts enabled', function (done) {
    const key = 'key';
    const value = 'value';
    const numMasterHosts = 1;
    const options = { writeTimeout: 1000 };
    const redisHosts = global.generateRedisHosts(numMasterHosts);
    const shardedClient = new ShardedRedis(redisHosts, options);

    spyOn(MockRedisClient.prototype, 'set');

    shardedClient.set(key, value, (err) => {
      expect(err instanceof Error).toBeTrue();
      expect(MockRedisClient.prototype.set).toHaveBeenCalledTimes(1);
      expect(err.message).toBe('Redis call timed out');
      done();
    });

    jasmine.clock().tick(1000);
  });

  it('should only call the cb once if a timeout is not triggered', function (done) {
    const key = 'key';
    const numMasterHosts = 1;
    const options = { readTimeout: 500 };
    const redisHosts = global.generateRedisHosts(numMasterHosts);
    const shardedClient = new ShardedRedis(redisHosts, options);
    const mainCbSpy = jasmine.createSpy('mainCb').and.callFake((err) => expect(MockRedisClient.prototype.get).toHaveBeenCalledTimes(1));

    spyOn(MockRedisClient.prototype, 'get').and.callThrough();

    shardedClient.get(key, mainCbSpy);
    setTimeout(() => expect(mainCbSpy).toHaveBeenCalledTimes(1) || done(), 600);

    jasmine.clock().tick(600);
  });

  it('should move on to the next client if redis client times out on reads', function (done) {
    const key = 'key';
    const numPorts = 1;
    const numSlaves = 2;
    const numMasterHosts = 1;
    const options = { readTimeout: 500 };
    const redisHosts = global.generateRedisHosts(numMasterHosts, numPorts, numSlaves);
    const shardedClient = new ShardedRedis(redisHosts, options);
    const wrappedClient = shardedClient._getWrappedClient(key);
    const masterClient = wrappedClient.get();
    const slaveClient1 = wrappedClient.getSlave();
    const slaveClient2 = wrappedClient.getSlave();

    spyOn(masterClient, 'get').and.callThrough();
    spyOn(slaveClient1, 'get');
    spyOn(slaveClient2, 'get');

    shardedClient.get(key, (err) => {
      expect(err).toBeUndefined();
      expect(slaveClient1.get).toHaveBeenCalledTimes(1);
      expect(slaveClient2.get).toHaveBeenCalledTimes(1);
      expect(masterClient.get).toHaveBeenCalledTimes(1);
      done();
    });

    jasmine.clock().tick(1000);
  });
});
