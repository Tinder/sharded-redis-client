/********************************
 * Test runner setup
 ********************************/

// Include extra matchers
require('jasmine-expect');

// Set up handy noop utility.
// Spy on this function with spyOn(global, 'noop');
// Spies are reset after each test, so you can spy
// on this function once per test.
global.noop = function () {};

// Utility for creating callback functions.
// Automatically calls callback functions without error
// if given.
global.cb = function () {
  return function () {
    var cb = arguments[arguments.length - 1];
    if (typeof cb === 'function') {
      cb();
    }
  };
};

// Override the native setImmediate so that
// jasmine.clock().tick(milliseconds) works
global.setImmediate = function (callback) {
  setTimeout(callback, 1);
};
