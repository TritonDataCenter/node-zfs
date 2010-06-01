#!/usr/bin/env node

sys = require('sys');
fs = require('fs');

zfs = require('./zfs').zfs;
zpool = require('./zfs').zpool;
puts = sys.puts;
inspect = sys.inspect;

TestSuite = require('./async-testing/async_testing').TestSuite;

function assertDatasetExists(assert, name, callback) {
  var listFunc = name.indexOf('@') === -1
                 ? zfs.list
                 : zfs.list_snapshots;

  listFunc(name, function (err, fields, list) {
    assert.ok(!err, "There was an error");
    assert.ok(list, "Checking that we got a list of datasets.");
    assert.ok(list.length > 0, "zfs list is empty");
    assert.ok(list.some(function (i) { return i[0] === name; }),
       "zfs dataset doesn't exist");
    callback();
  });
}

function assertDatasetDoesNotExist(assert, name, callback) {
  var listFunc = name.indexOf('@') === -1
                 ? zfs.list
                 : zfs.list_snapshots;
  listFunc(name, function (err, fields, list) {
    assert.ok(err, "expected an error but didn't get one");
    assert.ok(err.msg.match(/does not exist/), "received unexpected error message " + err.msg);
    assert.ok(!list, "zfs list is empty");
    callback();
  });
}

var zfsName = process.argv[2] || 'foobar/test';
var zpoolName = zfsName.split('/')[0];
var testFilename = '/' + zfsName + '/mytestfile';
var testData = "Dancing is forbidden!";
var testDataModified = "Chicken arise! Arise chicken! Arise!";
var suite = new TestSuite("node-zfs unit tests");

var tests = [
  { 'pre check':
    function (assert, finished) {
      zpool.list(function (err, fields, list) {
        assert.ok(list, 'zpools list was empty or did not have a value');
        assert.ok(list.length > 0, "zpool list is empty");
        assert.ok(
          list.some(function (i) { return i[0] == zpoolName; }),
          "zpool doesn't exist");

        zfs.list(function (err, fields, list) {
          assert.ok(list, 'zfs list was empty or did not have a value');
          assert.ok(list.length > 0, "zfs list is empty");
          assert.ok(
            !list.some(function (i) { return i[0] == zfsName; }),
            "zfs dataset already exists");

          finished();
        });
      });
    }
  }
, { 'create a dataset':
    function (assert, finished) {
      zfs.create(zfsName, function () {
        assertDatasetExists(assert, zfsName, function() {
          fs.writeFile(testFilename, testData, function (error) {
            if (error) throw error;
            finished();
          });
        });
      });
    }
  }
, { "set'ing a property":
    function (assert, finished) {
      var properties = { 'test:property1': "foo\tbix\tqube"
                       , 'test:property2': 'baz'
                       };
      zfs.set(zfsName, properties, finished);
    }
  }
, { "get'ing a property":
    function (assert, finished) {
      zfs.get(zfsName,
        ['test:property1', 'test:property2'],
        function (err, properties) {
          assert.ok(properties, "Didn't get any properties back");
          assert.equal(properties['test:property1'], "foo\tbix\tqube",
            "Property 'test:property1' should be 'foo'");
          assert.equal(properties['test:property2'], 'baz',
            "Property 'test:property2' should be 'baz'");
          finished();
        });
    }
  }
, { "snapshot a dataset":
    function (assert, finished) {
      var snapshotName = zfsName + '@mysnapshot';
      zfs.snapshot(snapshotName, function (error, stdout, stderr) {
        if (error) throw error;
        assertDatasetExists(assert, snapshotName, function () {
          // check that the snapshot appears in the `list_snapshots` list
          zfs.list_snapshots(function (err, fields, lines) {
            assert.ok(
              lines.some(function (i) { return i[0] === snapshotName; }),
              "snapshot didn't appear in list of snapshots");

            // check that the snapshot didn't appear in the `list` list
            zfs.list(function (err, fields, lines) {
              assert.ok(
                !lines.some(function (i) { return i[0] === snapshotName; }),
                "snapshot appeared in `list` command");
              finished();
            });
          });
        });
      });
    }
  }
, { 'rolling back a snapshot':
    function (assert, finished) {
      var snapshotName = zfsName + '@mysnapshot';
      fs.writeFile(testFilename, testDataModified,
        function (error) {
          if (error) throw error;
          fs.readFile(testFilename, function (err, str) {
            if (err) throw err;
            assert.equal(str.toString(), testDataModified);
            zfs.rollback(snapshotName, function (err, stdout, stderr) {
              if (err) throw err;
              fs.readFile(testFilename, function (err, str) {
                assert.equal(str.toString(), testData);
                finished();
              });
            });
          });
        });
    }
  }
, { 'clone a dataset':
    function (assert, finished) {
      var snapshotName = zfsName + '@mysnapshot';
      var cloneName = zpoolName + '/' + 'myclone';
      zfs.clone(snapshotName, cloneName, function (err, stdout, stderr) {
        assertDatasetExists(assert, cloneName, finished);
      });
    }
  }
, { 'destroy a clone':
    function (assert, finished) {
      var cloneName = zpoolName + '/' + 'myclone';
      assertDatasetExists(assert, cloneName, function () {
        zfs.destroy(cloneName, function (err, stdout, stderr) {
          assertDatasetDoesNotExist(assert, cloneName, finished);
        });
      });
    }
  }
, { "destroying a snapshot":
    function (assert, finished) {
      var snapshotName = zfsName + '@mysnapshot';
      assertDatasetExists(assert, snapshotName, function () {
        zfs.destroy(snapshotName, function (err, stdout, stderr) {
          assertDatasetDoesNotExist(assert, snapshotName, finished);
        });
      });
    }
  }
, { "list errors":
    function (assert, finished) {
      var datasetName = 'thisprobably/doesnotexist';
      assertDatasetDoesNotExist(assert, datasetName, function () {
        zfs.list(datasetName, function (err, fields, list) {
          assert.ok(err);
          assert.ok(err.msg.match(/does not exist/),
            'Could list snashot that should not exist');
          finished();
        });
      });
    }
  }
, { "delete errors":
    function (assert, finished) {
      var datasetName = 'thisprobably/doesnotexist';
      assertDatasetDoesNotExist(assert, datasetName, function () {
        zfs.destroy(datasetName, function (err, stdout, stderr) {
          assert.ok(err, "Expected an error deleting nonexistant dataset");
          assert.ok(typeof(err.code) === 'number');
          assert.ok(err.code !== 0, "Return code should be non-zero");
          assert.ok(stderr.match(/does not exist/),
            'Error message did not indicate that dataset does not exist');
          finished();
        });
      });
    }
  }
];

var testCount = tests.length;

// order matters in our tests
for (i in tests) {
  suite.addTests(tests[i]);
}

suite.runTests();
