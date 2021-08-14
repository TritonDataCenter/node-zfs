/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var test = require('tap').test;
var util = require('util');
var fs = require('fs');
var path = require('path');
try {
	var zutil = require('/usr/node/node_modules/zutil');
} catch (_) {
	zutil = {
		getZone: function () {
			return ('other');
		}
	};
}

var puts = util.puts;
var inspect = util.inspect;

var zfsName = process.argv[2] || 'nodezfstest/test';
var zpoolName = zfsName.split('/')[0];
var testFilename = '/' + zfsName + '/mytestfile';
var testData = 'Dancing is forbidden!';
var testDataModified = 'Chicken arise! Arise chicken! Arise!';

var zfs = require('../lib/zfs').zfs;
var zpool = require('../lib/zfs').zpool;

test('basic', function (t) {
	t.ok(zfs, 'zfs module should exist');
	t.ok(zpool, 'zpool module should exist');
	t.end();
});

test('zfs', { skip: (zutil.getZone() !== 'global') }, function (t) {
	var datasetExists = function (_t, name, callback) {
		var listFunc = name.indexOf('@') === -1 ?
		    zfs.list : zfs.list_snapshots;

		listFunc(name, function (err, fields, list) {
			_t.notOk(err, 'dataset listing failed');
			_t.ok(list, 'no dataset list was returned');
			_t.ok(list.length > 0, 'dataset list is empty');
			_t.ok(list.some(function (d) {
				return (d[0] === name);
			}), 'zfs dataset ' + name + ' does not exist');
			callback();
		});
	};

	var noDatasetExists = function (_t, name, callback) {
		var listFunc = name.indexOf('@') === -1 ?
		    zfs.list : zfs.list_snapshots;

		listFunc(name, function (err, fields, list) {
			_t.ok(err, 'expected error did not occur');
			_t.ok(err.toString().match(/does not exist/),
			    'received unexpected error message ' + err.msg);
			_t.notOk(list, 'dataset list is not empty');
			callback();
		});
	};

	t.test('list pools', function (st) {
		zpool.list(function (err, fields, list) {
			st.notOk(err, 'zpool list failed: ' + err);
			st.ok(list, 'no zpool list was returned');
			st.ok(list.length > 0, 'zpool list is empty');
			st.ok(list.some(
			    function (p) { return (p[0] == zpoolName); }),
			    'zpool does not exist');
		});
		st.end();
	});

	t.test('list datasets', function (st) {
		zfs.list(function (err, fields, list) {
			st.notOk(err, 'dataset list failed: ' + err);
			st.ok(list, 'no dataset list was returned');
			st.ok(list.length > 0, 'dataset list is empty');
			st.notOk(list.some(
			    function (d) { return d[0] == zfsName; }),
			    'zfs dataset ' + zfsName + ' already exists');
		});
		st.end();
	});

	t.test('create dataset', function (st) {
		zfs.create(zfsName, function () {
			datasetExists(st, zfsName, function () {
				fs.writeFile(testFilename, testData,
				    function (err) {
					if (err)
						throw err;
					st.end();
				});
			});
		});
	});

	t.test('set property', function (st) {
		var properties = {
			'test:property1': 'foo\tbix\tqube',
			'test:property2': 'baz'
		};
		zfs.set(zfsName, properties, function (err) {
			st.notOk(err, 'setting properties failed: ' + err);
			st.end();
		});
	});

	t.test('get property', function (st) {
		var val;

		zfs.get(zfsName, ['test:property1', 'test:property2'],
		    false, function (err, properties) {
			st.ok(properties, 'no properties were returned');
			val = properties['test:property1'];
			st.equal(val, 'foo\tbix\tqube',
			    'property test:property1 has incorrect value "' +
			    val + '"');
			val = properties['test:property2'];
			st.equal(val, 'baz',
			    'property test:property2 has incorrect value ' +
			    val + '"');
			st.end();
		});
	});

	t.test('take snapshot', function (st) {
		var snapshotName = zfsName + '@mysnapshot';

		zfs.snapshot(snapshotName, function (err, stdout, stderr) {
			st.notOk(err, 'snapshot error occurred');
			zfs.list_snapshots(function (serr, fields, lines) {
				st.ok(lines.some(function (s) {
					return (s[0] === snapshotName); }),
				    'snapshot not found');
				zfs.list(function (sserr, sfields, slines) {
					st.ok(lines,
					    'no dataset list was returned');
					st.notOk(lines.some(function (d) {
						return (d[0] === snapshotName);
					}),
					'snapshot found in zfs.list result');
					st.end();
				});
			});
		});
	});

	t.test('recursive dataset list', function (st) {
		function inList(needle, haystack) {
			return (haystack.some(function (i) {
				return (needle === i[0]);
			}));
		}

		zfs.list(zfsName, { recursive: true, type: 'all' },
		    function (error, fields, list) {
			st.equal(list.length, 2, 'dataset list has length ' +
			    list.length);
			st.ok(inList(zfsName, list),
			    zfsName + ' not found in list');
			st.ok(inList(zfsName + '@mysnapshot', list),
			    zfsName + '@mysnapshot not found in list');
			st.end();
		});
	});

	t.test('send a snapshot to a file', function (st) {
		var snapshotName = zfsName + '@mysnapshot';
		var snapshotFilename = '/tmp/node-zfs-test-snapshot.zfs';

		zfs.send(snapshotName, snapshotFilename, function () {
			path.exists(snapshotFilename, function (exists) {
				st.ok(exists, 'no output file exists');
				st.end();
			});
		});
	});

	t.test('receive a snapshot from a file', function (st) {
		var datasetName = zfsName + '/from_receive';
		var snapshotFilename = '/tmp/node-zfs-test-snapshot.zfs';

		zfs.receive(datasetName, snapshotFilename, function (err) {
			st.notOk(err, 'zfs.receive failed: ' + err);
			datasetExists(t, datasetName, function () {
				path.exists('/' + datasetName + '/mytestfile',
				    function (exists) {
					st.ok(exists, 'snap file went away');
					fs.readFile('/' + datasetName +
					    '/mytestfile', function (e, str) {
						st.notOk(e,
						    'read test file failed: ' +
						    e);
						st.equal(str.toString(),
						    testData);
						st.end();
					});
				});
			});
		});
	});

	t.test('snapshot rollback', function (st) {
		var snapshotName = zfsName + '@mysnapshot';

		fs.writeFile(testFilename, testDataModified,
		    function (err) {
			st.notOk(err, 'write test file failed: ' + err);
			fs.readFile(testFilename, function (serr, str) {
				st.notOk(err, 'read test file failed: ' + serr);
				st.equal(str.toString(), testDataModified);
				zfs.rollback(snapshotName,
				    function (sserr, stdout, stderr) {
					st.notOk(sserr,
					    'rollback failed: ' + sserr);
					fs.readFile(testFilename,
					    function (ssserr, sstr) {
						st.equal(sstr.toString(),
						    testData);
						st.end();
					});
				});
			});
		});
	});

	t.test('create clone', function (st) {
		var snapshotName = zfsName + '@mysnapshot';
		var cloneName = zpoolName + '/' + 'myclone';

		zfs.clone(snapshotName, cloneName,
		    function (err, stdout, stderr) {
			datasetExists(st, cloneName, function () {
				st.end();
			});
		});
	});

	t.test('destroy clone', function (st) {
		var cloneName = zpoolName + '/' + 'myclone';

		datasetExists(t, cloneName, function () {
			zfs.destroy(cloneName, function (err, stdout, stderr) {
				noDatasetExists(t, cloneName, function () {
					st.end();
				});
			});
		});
	});

	t.test('destroy snapshot', function (st) {
		var snapshotName = zfsName + '@mysnapshot';

		datasetExists(st, snapshotName, function () {
			zfs.destroy(snapshotName,
			    function (err, stdout, stderr) {
				noDatasetExists(t, snapshotName,
				    function () {
					st.end();
				});
			});
		});
	});

	t.test('destroy dataset', function (st) {
		zfs.destroyAll(zfsName, function (err, stdout, stderr) {
			noDatasetExists(st, zfsName, function () {
				st.end();
			});
		});
	});

	t.test('list errors', function (st) {
		var datasetName = 'thisprobably/doesnotexist';

		noDatasetExists(t, datasetName, function () {
			zfs.list(datasetName, function (err, fields, list) {
				st.ok(err, 'error expected but did not occur');
				st.ok(err.toString().match(/does not exist/),
				    'bad error on nonexistent dataset: ' + err);
				st.end();
			});
		});
	});

	t.test('delete errors', function (st) {
		var datasetName = 'thisprobably/doesnotexist';

		noDatasetExists(st, datasetName, function () {
			zfs.destroy(datasetName,
			    function (err, stdout, stderr) {
				st.ok(err, 'no error deleting nonexistent ds');
				st.ok(err.toString().match(/does not exist/),
				    'bad error message on deletion: ' + err);
				st.end();
			});
		});
	});
});

function
check_layout(dl, t, name, layout)
{
	var disks = [];
	var config;
	var correct;
	var cfname = 'config.' + name;

	if (layout)
		cfname += '.' + layout;

	correct = JSON.parse(fs.readFileSync(cfname, 'utf8'));

	t.test(name, function (st) {
		fs.readFile('diskinfo.' + name, 'utf8', function (err, data) {
			var lines;

			if (err)
				throw err;

			lines = data.trim().split('\n');
			lines.forEach(function (line) {
				if (line) {
					var row = line.split('\t');
					disks.push({
						type: row[0],
						name: row[1],
						vid: row[2],
						pid: row[3],
						size: row[4],
						removable: (row[5] === 'yes'),
						solid_state: (row[6] === 'yes')
					});
				}
			});

			config = dl.compute(disks, layout);
			st.deepEqual(config, correct);
			st.end();
		});
	});
}

// These were broken by the smartos sync.
test('disklayout', { skip: true }, function (t) {
	var disklayout = require('../lib/disklayout');

	t.ok(disklayout, 'disklayout module should exist');

	check_layout(disklayout, t, 'single', undefined);
	check_layout(disklayout, t, 'mirror', undefined);
	check_layout(disklayout, t, 'removable', undefined);
	check_layout(disklayout, t, 'single.ssd', undefined);
	check_layout(disklayout, t, 'ssd', undefined);
	check_layout(disklayout, t, 'coal', undefined);
	check_layout(disklayout, t, 'dell', undefined);
	check_layout(disklayout, t, 'richmond', undefined);
	check_layout(disklayout, t, 'ms', undefined);
	check_layout(disklayout, t, 'ms', 'mirror');
	check_layout(disklayout, t, 'richmond', 'raidz2');

	t.end();
});
