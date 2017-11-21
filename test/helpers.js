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

global.generateRedisHosts = (numMasters, numPorts, numSlaves) => {
  const hosts = [];
  let counter = 0;

  for (let m = 0; m < numMasters; ++m) {
    const masterConfig = generateRedisConfig(numPorts);

    for (let s = 0; s < numSlaves; ++s) {
      if (!masterConfig.slaveHosts)
        masterConfig.slaveHosts = [];

      masterConfig.slaveHosts.push(generateRedisConfig().host);
      masterConfig.readPreference = 'slave';
    }

    hosts.push(masterConfig);
  }

  return hosts;

  function generateRedisConfig(numPorts) {
    const portRange = [6379];

    if (numPorts > 1)
      portRange.push(6379 + numPorts - 1);

    const config = {
      host: 'http://hostname' + counter++,
      port_range: portRange
    };

    return config;
  }
};
