#!/usr/bin/env node

var sys = require('sys')
  , assert = require('assert')
  , zfs = require('./zfs').zfs
  , zpool = require('./zfs').zpool;

// assert test is running as root

TestPlanner = function (testCount) {
  this.count = 0;
  var self = this;
  var aborted = false;
  var onExit = function (error) {
    if (aborted) return;
    aborted = true;

    if (error) { 
      puts(error.stack);
    }
    if (self.teardown) {
      puts("Tearing down...");
      self.teardown();
    }
    assert.equal(testCount, self.count,
                 'Number of tests run (' + self.count
                 + ') didn\'t match number of tests planned (' + testCount + ')');
    puts("All tests successful!");
  };

  process.addListener('exit', onExit);
  process.addListener('uncaughtException', onExit);
};

TestPlanner.prototype.track = function (fn) {
  var self = this;
  return function () {
    puts("test-");
    self.count++;
    return fn.apply(undefined, arguments);
  };
};

var testPlan = 18;
var tp = new TestPlanner(testPlan);

tp.teardown = function () {
  puts("tearing down");
  zfs.destroyAll(zfsName, function () {
    puts("destroyed " + zfsName + inspect(arguments));
  });
}

ok = tp.track(assert.ok);
equal = tp.track(assert.equal);

var puts = sys.puts;
var inspect = sys.inspect;

var zfsName = process.argv[2] || 'node-zfs-test/test';
var zpoolName = zfsName.split('/')[0];

function preCheck() {
  // check zpool exists
  zpool.list(function (err, fields, list) {
    ok(list, 'zpools list was empty or did not have a value');
    ok(list.length > 0, "zpool list is empty");
    ok(list.some(function (i) { return i[0] == zpoolName; }),
       "zpool doesn't exist");

    zfs.list(function (err, fields, list) {
      ok(list, 'zfs list was empty or did not have a value');
      ok(list.length > 0, "zfs list is empty");
      ok(!list.some(function (i) { return i[0] == zfsName; }),
         "zfs dataset already exists");

      runTests();
    });
  });
}

function assertDatasetExists(name, callback) {
  zfs.list(name, function (err, fields, list) {
    ok(list.length > 0, "zfs list is empty");
    ok(list.some(function (i) { return i[0] == name; }),
       "zfs dataset doesn't exist");
    callback();
  });
}

function assertDatasetDoesNotExist(name, callback) {
  zfs.list(name, function (err, fields, list) {
    ok(err);
    ok(err.msg.match(/does not exist/));
    ok(!list, "zfs list is empty");
    callback();
  });
}

assertDatasetExists = tp.track(assertDatasetExists);
assertDatasetDoesNotExist = tp.track(assertDatasetDoesNotExist);

function runTests() {
  var tp = 0;

  var tests =
    [ function () {
        zfs.create(zfsName, function () {
          assertDatasetExists(zfsName, next);
        });
      }
    , function () {
      var snapshotName = zfsName + '@mysnapshot';
      zfs.snapshot(snapshotName, function (err, stdout, stderr) {
        assertDatasetExists(snapshotName, next);
      });
    }
    , function () {
      var snapshotName = zfsName + '@mysnapshot';
      assertDatasetExists(snapshotName, function () {
        zfs.destroy(snapshotName, function (err, stdout, stderr) {
          assertDatasetDoesNotExist(snapshotName, next);
        });
      });
    }

    
//     , function () {}
//     , function () {}
    ];

  function next() {
    if (tp >= tests.length) return;

    puts("Starting new test");
    try {
      tests[tp++]();
    }

    catch(e) {
      puts("Error: " + e.toString());
      puts(e.back);
      next();
    }
  }

  next();
}

function test() {
  preCheck();
}

test();
