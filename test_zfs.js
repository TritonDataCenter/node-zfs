#!/usr/bin/env node

var sys = require('sys')
  , assert = require('assert')
  , zfs = require('./zfs').zfs
  , zpool = require('./zfs').zpool
  , fs = require('fs');

// assert test is running as root

TestPlanner = function (testCount) {
  this.count = 0;
  var self = this;
  var aborted = false;
  var onExit = function (error) {
//     if (aborted) return;
//     aborted = true;
    if (error) { 
      puts(error.stack);
    }

    if (self.teardown) {
      self.teardown();
    }
    if (testCount !== self.count) {
      puts('Number of tests run (' + self.count
         + ') didn\'t match number of tests planned (' + testCount + ')');
    }
  };

  process.addListener('exit', onExit);
  process.addListener('uncaughtException', onExit);
};

TestPlanner.prototype.track = function (fn) {
  var self = this;
  return function () {
    self.count++;
    return fn.apply(undefined, arguments);
  };
};

var testPlan = 52;
var tp = new TestPlanner(testPlan);

tp.teardown = function () {
  puts("Tearing down");
  zfs.destroyAll(zfsName, function () {
    puts("destroyed " + zfsName + inspect(arguments));
  });
}

ok = tp.track(assert.ok);
equal = tp.track(assert.equal);

var puts = sys.puts;
var inspect = sys.inspect;

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
    ok(!err);
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

var zfsName = process.argv[2] || 'node-zfs-test/test';
var zpoolName = zfsName.split('/')[0];
var testFilename = '/' + zfsName + '/mytestfile';
var testData = "Dancing is forbidden!";
var testDataModified = "Chicken arise! Arise chicken! Arise!";

function runTests() {
  var tp = 0;

  var tests = [
    // Test create dataset
    function () {
      zfs.create(zfsName, function () {
        assertDatasetExists(zfsName, function() {
          fs.writeFile(testFilename, testData, function (error) {
            if (error) throw error;
            next();
          });
        });
      });
    }

    // Test snapshots
    , function () {
      var snapshotName = zfsName + '@mysnapshot';
      zfs.snapshot(snapshotName, function (error, stdout, stderr) {
        if (error) throw error;
        assertDatasetExists(snapshotName, function () {
          // check that the snapshot appears in the `list_snapshots` list
          zfs.list_snapshots(function (err, fields, lines) {
            ok(lines.some(function (i) { return i[0] === snapshotName; }),
               "snapshot didn't appear in list of snapshots");

            // check that the snapshot didn't appear in the `list` list
            zfs.list(function (err, fields, lines) {
              ok(!lines.some(function (i) { return i[0] === snapshotName; }),
                 "snapshot appeared in `list` command");
              next();
            });
          });
        });
      });
    }

    // Test rolling back to a snapshot
    , function () {
      var snapshotName = zfsName + '@mysnapshot';
      fs.writeFile(testFilename, testDataModified,
        function (error) {
          if (error) throw error;
          fs.readFile(testFilename, function (err, str) {
            if (err) throw err;
            equal(str, testDataModified);
            zfs.rollback(snapshotName, function (err, stdout, stderr) {
              if (err) throw err;
              fs.readFile(testFilename, function (err, str) {
                equal(str, testData);
                next();
              });
            });
          });
        });
    }

    // Test cloning
    , function () {
      var snapshotName = zfsName + '@mysnapshot';
      var cloneName = zpoolName + '/' + 'myclone';
      zfs.clone(snapshotName, cloneName, function (err, stdout, stderr) {
        assertDatasetExists(cloneName, next);
      });
    }

    // Test destroying a clone
    , function () {
      var snapshotName = zpoolName + '/' + 'myclone';
      assertDatasetExists(snapshotName, function () {
        zfs.destroy(snapshotName, function (err, stdout, stderr) {
          assertDatasetDoesNotExist(snapshotName, next);
        });
      });
    }

    // Test destroying a snapshot
    , function () {
      var snapshotName = zfsName + '@mysnapshot';
      assertDatasetExists(snapshotName, function () {
        zfs.destroy(snapshotName, function (err, stdout, stderr) {
          assertDatasetDoesNotExist(snapshotName, next);
        });
      });
    }

    // Test List error
    , function () {
      var snapshotName = 'thisprobably/doesnotexist';
      assertDatasetDoesNotExist(snapshotName, function () {
        zfs.list(snapshotName, function (err, fields, list) {
          ok(err);
          ok(err.msg.match(/does not exist/),
             'Could list snashot that should not exist');
          next();
        });
      });
    }

    // Test Delete error
    , function () {
      var snapshotName = 'thisprobably/doesnotexist';
      assertDatasetDoesNotExist(snapshotName, function () {
        zfs.destroy(snapshotName, function (err, stdout, stderr) {
          ok(err, "Expected an error deleting nonexistant dataset");
          ok(typeof(err.code) === 'number');
          ok(err.code !== 0, "Return code should be non-zero");
          ok(stderr.match(/does not exist/),
             'Error message did not indicate that dataset does not exist');
          next();
        });
      });
    }
  ];

  function next() {
    if (tp >= tests.length) return;

    puts(".");
    try {
      tests[tp++]();
    }
    catch(e) {
      puts("**** Error: " + e.toString());
      next();
    }
  }

  next();
}

function test() {
  preCheck();
}

test();
