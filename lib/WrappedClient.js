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

const _ = require('lodash');
const redis = require('redis');
const EventEmitter = require('events').EventEmitter;
const RoundRobinSet = require('./RoundRobinSet');
const Brakes = require('brakes');
const Promise = require('bluebird');

function createClient(port, host, options) {
  const client = redis.createClient(port, host, options.redisOptions);
  const interval = 150000;
  const timeout = Math.floor(Math.random() * (interval + 1));

  if (options.usePing)
    setTimeout(() => setInterval(() => client.ping(_.noop), interval), timeout);

  if (options.breakerConfig) {
    const brake = new Brakes((cmd, args) => new Promise((resolve, reject) => {
      args = _.cloneDeep(args);
      args[length - 1] = (err, result) => {
        if(err)
          return reject(err);
        return resolve(result);
      };

      return client[cmd].apply(client, args);
    }), options.breakerConfig);

    brake.on('circuitOpen', () => console.log(`Circuit Opened at ${new Date().toISOString()}`));
    brake.on('circuitClosed', () => console.log(`Circuit Closed at ${new Date().toISOString()}`));

    client._breaker = brake;
  }

  client.on('error', (e) => console.log(`Redis Error [${host}:${port}]: ${e} : ${new Date().toISOString()}`));
  client.on('end', (e) => console.log(`Redis End [${host}:${port}]: ${e} : ${new Date().toISOString()}`));

  return client;
}

module.exports = class WrappedClient extends EventEmitter {
  
  constructor(conf, options) {
    const client = createClient(conf.port, conf.host, options);
    const slaveClients = conf.slaves.map((slaveHost) => createClient(conf.port, slaveHost, options));

    if (!slaveClients.length)
      slaveClients.push(client);

    client._isMaster = true;

    super();

    this.client = client;
    this.slaves = new RoundRobinSet(slaveClients);
    this.readPreference = conf.readPreference;
  }

  get() {
    return this.client;
  }

  getSlave() {
    return this.slaves.obtain();
  }

};
