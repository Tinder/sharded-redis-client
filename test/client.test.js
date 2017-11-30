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
const SHARDABLE = require('../ShardedRedisClient').SHARDABLE;

describe('Test client', () => {

  const WrappedClient = proxyquire('../lib/WrappedClient', { redis: MockRedisClient });
  const ShardedRedis = proxyquire('../ShardedRedisClient', { './lib/WrappedClient': WrappedClient });

  it('should create a node_redis client when created', () => {
    const numMasterHosts = 1;
    const options = { redisOptions: {} };
    const redisHosts = global.generateRedisHosts(numMasterHosts);

    spyOn(MockRedisClient, 'createClient').and.callThrough();

    const shardedClient = new ShardedRedis(redisHosts, options);

    expect(MockRedisClient.createClient).toHaveBeenCalledTimes(1);
    expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[0].port_range[0], redisHosts[0].host, options.redisOptions);
  });

  it('should create master and slave clients', () => {
    const numMasterHosts = 1;
    const numPorts = 1;
    const numSlaves = 3;
    const redisHosts = global.generateRedisHosts(numMasterHosts, numPorts, numSlaves);

    spyOn(MockRedisClient, 'createClient').and.callThrough();

    const shardedClient = new ShardedRedis(redisHosts);

    expect(MockRedisClient.createClient).toHaveBeenCalledTimes(numSlaves + 1);
    expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[0].port_range[0], redisHosts[0].host, undefined);

    for (let i = 0; i < numSlaves; ++i)
      expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[0].port_range[0], redisHosts[0].slaveHosts[i], undefined);
  });

  it('should create multiple master clients', () => {
    const numMasterHosts = 3;
    const numPorts = 5;
    const redisHosts = global.generateRedisHosts(numMasterHosts, numPorts);

    spyOn(MockRedisClient, 'createClient').and.callThrough();

    const shardedClient = new ShardedRedis(redisHosts);

    expect(MockRedisClient.createClient).toHaveBeenCalledTimes(numMasterHosts * numPorts);

    for (let i = 0; i < numMasterHosts; ++i)
      for (let j = 0; j < numPorts; ++j)
        expect(MockRedisClient.createClient).toHaveBeenCalledWith(redisHosts[i].port_range[0] + j, redisHosts[i].host, undefined);
  });

  it('should call the corresponding redis command on the redis client', (done) => {
    const numMasterHosts = 1;
    const shardedClient = new ShardedRedis(global.generateRedisHosts(numMasterHosts));

    async.each(SHARDABLE, (cmd, cb) => {
      spyOn(MockRedisClient.prototype, cmd).and.callThrough();

      shardedClient[cmd]('key', () => {
        expect(MockRedisClient.prototype[cmd]).toHaveBeenCalledTimes(1);
        cb();
      });
    }, done);
  });

  it('should go to the same wrapped client when multiple requests are made with the same key', (done) => {
    const numMasterHosts = 3;
    const numPorts = 5;
    const callTimes = 5;
    const redisHosts = [];
    const key = 'key';
    const shardedClient = new ShardedRedis(global.generateRedisHosts(numMasterHosts, numPorts));
    const mockedRedisClient = shardedClient._getWrappedClient(key).get();

    spyOn(mockedRedisClient, 'get').and.callThrough();

    async.times(callTimes, (n, cb) => shardedClient.get(key, cb), () => {
      expect(mockedRedisClient.get).toHaveBeenCalledTimes(callTimes);
      done();
    });
  });

  it('should go to the same wrapped client if shard key is same as key', (done) => {
    const numMasterHosts = 3;
    const numPorts = 5;
    const callTimes = 5;
    const redisHosts = [];
    const shardKey = 'key';
    const badKey = 'badKey';
    const shardedClient = new ShardedRedis(global.generateRedisHosts(numMasterHosts, numPorts));
    const mockedRedisClient = shardedClient._getWrappedClient(shardKey).get();
    const mockedBadClient = shardedClient._getWrappedClient(badKey).get();

    spyOn(mockedRedisClient, 'get').and.callThrough();
    spyOn(mockedBadClient, 'get').and.callThrough();

    async.times(callTimes, (n, cb) => {
      async.series([
        (callback) => shardedClient.get(shardKey, callback),
        (callback) => shardedClient.getWithOptions({ shardKey }, badKey, callback)
      ], cb);
    }, () => {
      expect(mockedRedisClient.get).toHaveBeenCalledTimes(callTimes * 2);
      done();
    });
  });

  it('should not always go to the same redis client', (done) => {
    const numMasterHosts = 3;
    const numPorts = 3;
    const numKeys = 100;
    const shardedClient = new ShardedRedis(global.generateRedisHosts(numMasterHosts, numPorts));
    const wrappedClients = shardedClient._wrappedClients;
    let currKeyNum = 0;

    for (let i = 0; i < numMasterHosts * numPorts; ++i)
      spyOn(wrappedClients[i].get(), 'get').and.callThrough();

    async.times(numKeys, (n, cb) => shardedClient.get('key' + currKeyNum++, cb), () => {
      let sumCalls = 0;

      for (let i = 0; i < numMasterHosts * numPorts; i++) {
        sumCalls += wrappedClients[i].get().get.calls.count();
        expect(wrappedClients[i].get().get.calls.count()).toBeLessThan(numKeys);
      }

      expect(sumCalls).toBe(numKeys);

      done();
    });
  });

  it('should try to read from slaves first when readPreference is "slave"', (done) => {
    const key = 'key';
    const numMasterHosts = 1;
    const numPorts = 1;
    const numSlaveHosts = 5;
    const shardedClient = new ShardedRedis(generateRedisHosts(numMasterHosts, numPorts, numSlaveHosts));
    const wrappedClient = shardedClient._getWrappedClient(key);
    const masterClient = wrappedClient.get();
    const slaveClients = [];
    let totalCalls = 0;
    let masterCallNumber = 0;

    for (let i = 0; i < numSlaveHosts; ++i) {
      const sc = wrappedClient.getSlave();

      slaveClients.push(sc);

      spyOn(sc, 'get').and.callFake((key, cb) => ++totalCalls && cb(new Error('an error')));
    }

    spyOn(masterClient, 'get').and.callFake((key, cb) => {
      ++totalCalls;
      masterCallNumber = totalCalls;
      cb();
    });

    shardedClient.get(key, () => {
      slaveClients.forEach((sc) => {
        expect(sc.get).toHaveBeenCalledTimes(1);
        expect(sc.get.calls.argsFor(0).slice(0, -1)).toEqual([key]);
      });

      expect(masterClient.get).toHaveBeenCalledTimes(1);
      expect(masterClient.get.calls.argsFor(0).slice(0, -1)).toEqual([key]);
      expect(masterCallNumber).toEqual(totalCalls);
      done();
    });
  });

  it('should only make write calls to master, even if readPreference is "slave"', (done) => {
    const key = 'key';
    const value = 'value';
    const numMasterHosts = 1;
    const numPorts = 1;
    const numSlaveHosts = 5;
    const shardedClient = new ShardedRedis(generateRedisHosts(numMasterHosts, numPorts, numSlaveHosts));
    const wrappedClient = shardedClient._getWrappedClient(key);
    const masterClient = wrappedClient.get();
    const slaveClients = [];

    for (let i = 0; i < numSlaveHosts; ++i) {
      const sc = wrappedClient.getSlave();

      slaveClients.push(sc);
      spyOn(sc, 'set').and.callThrough();
    }

    spyOn(masterClient, 'set').and.callThrough();

    shardedClient.set(key, value, () => {
      slaveClients.forEach((sc) => expect(sc.set).not.toHaveBeenCalled());

      expect(masterClient.set).toHaveBeenCalledTimes(1);
      expect(masterClient.set.calls.argsFor(0).slice(0, -1)).toEqual([key, value]);

      done();
    });
  });

});
