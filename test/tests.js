var Jasmine = require('jasmine');
var jasmine = new Jasmine();

// To customize reporter: https://github.com/bcaudan/jasmine-spec-reporter
var SpecReporter = require('jasmine-spec-reporter');
var noop = function () {};

// Default jasmine spec reporter options
var reporter = {
  displayStacktrace: 'specs',
  displayPendingSummary: false
};

jasmine.configureDefaultReporter({ print: noop });
jasmine.addReporter(new SpecReporter(reporter));
jasmine.loadConfig({
  spec_dir: '.',
  spec_files: [
    '**/*.test.js'
  ],
  helpers: [
    'setup.js'
  ]
});

// Run specific tests
var tests = [];
//if (process.argv.length > 2) {
//  tests = process.argv.slice(2);
//}

jasmine.execute(tests);
