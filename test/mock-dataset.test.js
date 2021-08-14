/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */
'use strict';

const tap = require('tap');
const mockds = require('../lib/mock-dataset.js');
const mockfs = require('mock-fs');
const sprintf = require('sprintf-js').sprintf;
const fs = require('fs');
const { VError } = require('verror');

const Dataset = mockds.Dataset;

function check_verror(t, testcb, name, message) {
	/*JSSTYLED*/
	message = message || `${testcb.name} throws VError with name=${name}`;

	try {
		testcb();
		t.ok(false, message);
	} catch (err) {
		var extra = { stack: err.stack };

		t.type(err, VError, message + ' (check type)', extra);
		t.strictSame(name, err.name, message + ' (check message)',
		    extra);
	}
}

tap.test('Dataset', function (tt) {
	tt.afterEach(function (done) {
		mockds.reset();
		done();
	});

	tt.test('root dataset', function (t) {
		let all = new Set();
		let root = new Dataset(null, 'root', 'filesystem');
		t.equals(root.name, 'root');
		all.add(root.name);
		let other = new Dataset(null, 'foo', 'filesystem');
		t.equals(other.name, 'foo');
		all.add(other.name);
		check_verror(t, function () {
			new Dataset(null, 'root', 'filesystem')
		}, 'DatasetExistsError', 'do not allow duplicate pools');
		check_verror(t, function () {
			new Dataset(null, 'foo', 'filesystem')
		}, 'DatasetExistsError', 'do not allow duplicate pools');

		let snap1 = root.snapshot('snap1');
		t.equals(snap1.name, 'root@snap1');
		all.add(snap1.name);

		let snap2 = root.snapshot('snap2', {recursive: true});
		t.equals(snap2.name, 'root@snap2');
		all.add(snap2.name);

		let snap3 = other.snapshot('snap3', {recursive: true});
		t.equals(snap3.name, 'foo@snap3');
		all.add(snap3.name);

		for (var ds of root.iterDescendants(['all'])) {
			t.ok(ds.name.startsWith('root'),
			    /*JSSTYLED*/
			    `${ds.name} is in the foo pool`);
			/*JSSTYLED*/
			t.ok(all.delete(ds.name), `${ds.name} was found`);
		}
		for (ds of other.iterDescendants(['all'])) {
			t.ok(ds.name.startsWith('foo'),
			    /*JSSTYLED*/
			    `${ds.name} is in the foo pool`);
			/*JSSTYLED*/
			t.ok(all.delete(ds.name), `${ds.name} was found`);
		}
		t.equal(all.size, 0, 'iterated all datasets');

		t.doesNotThrow(function () { snap3.destroy(); },
			'zfs destroy foo@snap3');
		t.doesNotThrow(function () { other.destroy(); },
		    'zfs destroy foo');
		t.doesNotThrow(function () { root.destroy({recursive: true}); },
			'zfs destroy -r root');
		var seen = false
		check_verror(t, function () {
			for (var _ of root.iterDescendants(['all'])) {
				seen = true;
			}
		}, 'InactiveDatasetError', 'cannot iterate delete dataset');
		t.equals(seen, false, 'destroyed dataset not iterated');

		check_verror(t, function () {
			new Dataset(null, 'upto11', 'volume');
		}, 'DatasetTypeError', 'top level volume not allowed');

		t.end();
	});

	tt.test('misc sad paths', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');

		check_verror(t, function () {
			new Dataset(root, 'badtype', 'fs');
		}, 'DatasetTypeError', 'no dataset for type=fs');

		check_verror(t, function () {
			new Dataset(root, 'badtype', 'zvol');
		}, 'DatasetTypeError', 'no dataset for type=zvol');

		let vol1 = new Dataset(root, 'vol1', 'volume');
		check_verror(t, function () {
			new Dataset(vol1, 'badfs', 'filesystem');
		}, 'DatasetTypeError', 'cannot create root/vol1/badfs');

		check_verror(t, function () {
			new Dataset(vol1, 'badvol', 'volume');
		}, 'DatasetTypeError', 'cannot create root/vol1/badvol');

		t.end();
	});

	tt.test('dataset names', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');

		check_verror(t, function () {
			new Dataset(root, [ 'blah' ], 'filesystem');
		}, 'DatasetNameError', 'dataset name cannot be an array');

		check_verror(t, function () {
			new Dataset(root, null, 'filesystem');
		}, 'DatasetNameError', 'dataset name cannot be null');

		check_verror(t, function () {
			new Dataset(root, '', 'filesystem');
		}, 'DatasetNameError',
		    'dataset name cannot be an empty string');

		check_verror(t, function () {
			new Dataset(root, 'a'.repeat(256), 'filesystem');
		}, 'DatasetNameError', 'dataset name cannot 256 characters');

		t.doesNotThrow(function () {
			let ds = new Dataset(root, 'a'.repeat(255),
			    'filesystem');
			ds.destroy();
		}, 'dataset name can be 255 characters');

		t.end();
	});

	tt.test('dataset iterator', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');
		let ds, found, expect;

		function check(types, top) {
			top = top || root;
			for (ds of top.iterDescendants(types)) {
				t.ok(!found.has(ds),
				    /*JSSTYLED*/
				    `${ds.name} not previously found`);
				/*JSSTYLED*/
				t.ok(expect.has(ds), `${ds.name} is expected`);
				if (expect.delete(ds)) {
					found.add(ds);
				}
			}
			for (ds of expect) {
				t.ok(false,
				    /*JSSTYLED*/
				    `${ds.name} was expected but not found`);
			}
		}

		found = new Set();
		expect = new Set([ root ]);
		check(['all']);

		found = new Set();
		expect = new Set([ root ]);
		check(['filesystem']);

		found = new Set();
		expect = new Set();
		check(['volume']);

		found = new Set();
		expect = new Set();
		check(['snapshot']);

		let snap1 = root.snapshot('snap1', {recursive: true});

		found = new Set();
		expect = new Set([ root, snap1 ]);
		check(['all']);

		found = new Set();
		expect = new Set([ root ]);
		check(['filesystem']);

		found = new Set();
		expect = new Set();
		check(['volume']);

		found = new Set();
		expect = new Set([ snap1 ]);
		check(['snapshot']);

		let fs1 = new Dataset(root, 'fs1', 'filesystem');
		let fs1a = new Dataset(fs1, 'a', 'filesystem');
		let fs1v = new Dataset(fs1, 'v', 'volume');

		found = new Set();
		expect = new Set([ root, snap1, fs1, fs1a, fs1v ]);
		check(['all']);

		found = new Set();
		expect = new Set([ root, fs1, fs1a ]);
		check(['filesystem']);

		found = new Set();
		expect = new Set([ fs1v ]);
		check(['volume']);

		found = new Set();
		expect = new Set([ snap1 ]);
		check(['snapshot']);

		// As before, but not starting from root
		found = new Set();
		expect = new Set([ fs1, fs1a, fs1v ]);
		check(['all'], fs1);

		found = new Set();
		expect = new Set([ fs1, fs1a ]);
		check(['filesystem'], fs1);

		found = new Set();
		expect = new Set([ fs1v ]);
		check(['volume'], fs1);

		found = new Set();
		expect = new Set([ ]);
		check(['snapshot'], fs1);

		// Recursive snapshot
		let snap2 = fs1.snapshot('snap2', { recursive: true });
		let snap2a = mockds.get('root/fs1/a@snap2');
		let snap2v = mockds.get('root/fs1/v@snap2');
		let snap3 = fs1a.snapshot('snap3');

		found = new Set();
		expect = new Set([
			root, snap1, fs1, fs1a, fs1v, snap2, snap2a, snap2v,
			snap3 ]);
		check(['all']);

		found = new Set();
		expect = new Set([ root, fs1, fs1a ]);
		check(['filesystem']);

		found = new Set();
		expect = new Set([ fs1v ]);
		check(['volume']);

		found = new Set();
		expect = new Set([ snap1, snap2, snap2a, snap2v, snap3 ]);
		check(['snapshot']);

		// Clones
		let fs2 = snap2.clone('root/fs2');
		t.ok(fs2, 'created root/fs2');

		found = new Set();
		expect = new Set([ root, fs1, fs1a, fs2 ]);
		check(['filesystem']);

		found = new Set();
		expect = new Set([ root, fs1, fs1a, fs2 ]);
		check(['filesystem', 'clones']);

		found = new Set();
		expect = new Set([ fs1, fs1a ]);
		check(['filesystem'], fs1);

		found = new Set();
		expect = new Set([ fs1, fs1a, fs2 ]);
		check(['filesystem', 'clones'], fs1);

		found = new Set();
		expect = new Set([
			root, snap1, fs1, fs1a, fs1v, snap2, snap2a, snap2v,
			snap3, fs2 ]);
		check(['all']);

		found = new Set();
		expect = new Set([
			root, snap1, fs1, fs1a, fs1v, snap2, snap2a, snap2v,
			snap3, fs2 ]);
		check(['all', 'clones']);

		found = new Set();
		expect = new Set([ fs1, fs1a, fs1v, snap2, snap2a, snap2v,
			snap3 ]);
		check(['all'], fs1);

		found = new Set();
		expect = new Set([
			fs1, fs1a, fs1v, snap2, snap2a, snap2v, snap3, fs2 ]);
		check(['all', 'clones'], fs1);

		t.end();
	});

	tt.test('snapshot', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');
		let snap1;

		t.doesNotThrow(function () { snap1 = root.snapshot('snap1'); },
			'zfs snapshot root@snap1');
		check_verror(t, function () { root.snapshot('snap1'); },
			'DatasetExistsError',
			'cannot create a second snapshot of the same name');
		check_verror(t, function () { snap1.snapshot('snap2'); },
			'DatasetTypeError', 'cannot snapshot a snapshot');
		check_verror(t, function () { root.snapshot('root@snap3'); },
			'DatasetNameError', 'snapname arg is the part after @');
		t.doesNotThrow(function () { snap1.rename('root@snap2'); },
			'zfs rename root@snap1 root@snap2');
		check_verror(t, function () { root.snapshot('snap2'); },
			'DatasetExistsError',
			'cannot create a second snapshot of the same name ' +
			'after rename');

		t.end();
	});

	tt.test('rename', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');
		let fs1 = new Dataset(root, 'fs1', 'filesystem');
		let snap1 = fs1.snapshot('snap1');

		t.doesNotThrow(function () { fs1.rename('root/fs2'); },
			'can rename root/fs1 -> root/fs2');
		t.equals(fs1.name, 'root/fs2', 'name is correct after rename');
		t.doesNotThrow(function () {
			let check = mockds.get('root/fs2@snap1');
			t.equals(check, snap1, 'found the right snap1');
			t.equals(check.name, 'root/fs2@snap1',
			    'snapshot name ok');
		}, 'can find root/fs2@snap');

		check_verror(t, function () { snap1.rename('root/notasnap'); },
			'InvalidArgumentError',
			'cannot rename a snapshot to name with out an \'@\'');

		t.end();
	});

	tt.test('clone', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');
		let fs1 = new Dataset(root, 'fs1', 'filesystem');
		var snap1 = fs1.snapshot('snap1');

		check_verror(t, function () { fs1.clone('root/noway'); },
			'DatasetTypeError',
			'cannot clone a filesystem (only snapshot)');

		check_verror(t, function () { snap1.clone('root/a/noway'); },
			'InvalidArgumentError',
			'clone requires parents=true if parent missing.');

		var fs2;
		t.doesNotThrow(function () { fs2 = snap1.clone('root/fs2'); },
			'zfs clone root/fs1@snap1 root/fs2');
		t.ok(fs2, 'snapshot returned a dataset');
		t.equal(fs2.name, 'root/fs2', 'root/fs2 has expected name');
		t.equal(fs2.type, 'filesystem', 'root/fs2 is a filesystem');
		t.equal(fs2.mountpoint, '/root/fs2', 'root/fs2 mountpoint');

		t.doesNotThrow(function () { fs2.destroy(); },
			'can destroy root/fs2');
		t.doesNotThrow(function () {
			fs2 = snap1.clone('root/fs2', {},
			    { mountpoint: '/blah' });
		}, 'zfs clone root/fs1@snap1 root/fs2, with custom mountpoint');
		t.equal(fs2.mountpoint, '/blah',
			'root/fs2 mountpoint is /blah');

		var fs3;
		var newname = 'root/foo/a/b/c/fs3'
		t.doesNotThrow(function () {
			fs3 = snap1.clone(newname, {parents: true});
		/*JSSTYLED*/
		}, `zfs clone root/fs1@snap1 ${newname}`);
		t.ok(fs3, 'snapshot returned a dataset');
		/*JSSTYLED*/
		t.equal(fs3.name, newname, `${newname} has expected name`);
		/*JSSTYLED*/
		t.equal(fs3.type, 'filesystem', `${newname} is a filesystem`);
		/*JSSTYLED*/
		t.equal(fs3.mountpoint, '/' + newname, `${newname} mountpoint`);

		let vol1 = new Dataset(root, 'vol1', 'volume');
		var snap, clone;
		t.doesNotThrow(function () { snap = vol1.snapshot('vsnap'); },
			'zfs snapshot root/vol1@vsnap');
		check_verror(t, function () { vol1.destroy(); },
			'DescendantError',
			'cannot destroy a volume with a snapshot');
		t.doesNotThrow(function () { clone = snap.clone('root/vol2'); },
			'zfs clone root/vol1@vsnap root/vol2');
		check_verror(t, function () {
				vol1.destroy({recursive: true});
			}, 'DependantError',
			'cannot recursive destroy a volume with a clone');

		t.end();
	});

	tt.test('holds', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');
		let fs1 = new Dataset(root, 'fs1', 'filesystem');
		let vol1 = new Dataset(root, 'vol1', 'volume');
		let fs1a = new Dataset(fs1, 'a', 'filesystem');
		let fs1ab = new Dataset(fs1a, 'b', 'filesystem');
		var snap1 = fs1.snapshot('snap1', {recursive: true});

		check_verror(t, function () { fs1.hold('nope'); },
			'DatasetTypeError', 'cannot hold a filesystem');
		check_verror(t, function () { vol1.hold('nope'); },
			'DatasetTypeError', 'cannot hold a volume');

		let fs1ab_snap1 = mockds.get(fs1ab.name + '@snap1');
		t.doesNotThrow(function () { fs1ab_snap1.hold('reason1'); },
			/*JSSTYLED*/
			`can create a hold on ${fs1ab_snap1.name}`);
		check_verror(t, function () { fs1ab_snap1.destroy(); },
			'SnapshotHoldError',
			'cannot destroy a snapshot with a hold');
		t.doesNotThrow(function () { fs1ab_snap1.release('reason1'); },
			/*JSSTYLED*/
			`can release a hold on ${fs1ab_snap1.name}`);
		t.doesNotThrow(function () {
			fs1ab_snap1.destroy();
			fs1ab_snap1 = null;
		}, `can destroy snapshot after hold released`);

		let fs1_snap1 = mockds.get(fs1.name + '@snap1');
		t.doesNotThrow(function () {
			fs1_snap1.hold('reason1', { recursive: true });
		/*JSSTYLED*/
		}, `can create a recursive snapshot on ${fs1_snap1.name}`);
		for (let ds of fs1.iterDescendants(['snapshot'])) {
			if (ds._name !== 'snap1') {
				continue;
			}
			t.ok(ds.holds().has('reason1'),
			/*JSSTYLED*/
			`${ds.name} has reason1 hold`);
		}

		t.doesNotThrow(function () {
			fs1ab.destroy();
			fs1ab = null;
		}, 'can destroy filesystem in hold hierarchy that has no ' +
		    'snapshots');

		let fs1a_snap1 = mockds.get(fs1a.name + '@snap1');
		check_verror(t, function () { fs1a_snap1.destroy(); },
			'SnapshotHoldError',
			'cannot destroy a snapshot with a hold');
		// Be sure state didn't flip from 'active' by using a getter.
		t.ok(fs1a_snap1.name, 'failed destroy did not harm snapshot');
		check_verror(t, function () {
				fs1a.destroy({ recursive: true });
			}, 'SnapshotHoldError',
			'cannot recursively destroy filesystem with a held ' +
			    'snapshot');

		t.doesNotThrow(function () {
			fs1_snap1.release('reason1', { recursive: true });
		}, 'can recursively release a hold');
		t.doesNotThrow(function () {
			fs1.destroy({ recursive: true });
		}, 'can recursively destroy after hold is released');

		t.end();
	});

	tt.test('properties', function (t) {
		let root = new Dataset(null, 'root', 'filesystem');
		let fs1 = new Dataset(root, 'fs1', 'filesystem');
		let fs2 = new Dataset(fs1, 'fs2', 'filesystem');
		let datasets = [ root, fs1, fs2 ];

		let rw_inheritable_props = [
			[ 'atime', [ 'on', 'off' ] ],
			[ 'canmount', [ 'on', 'off', 'noauto' ] ],
			[ 'checksum',
			    [ 'on', 'off' ] ],   // Other values legal, but meh.
			[ 'compression',
			    [ 'on', 'off' ] ],   // Other values legal, but meh.
			[ 'copies', [ 1, 2, 3 ] ]
		];

		let ro_props = [
			'createtxg',
			'creation',
			'guid',
			'name',
			'type'
		];

		var ds, prop, val, values;
		function getval() {
			val = ds[prop];
		}

		for (prop in rw_inheritable_props) {
			[ prop, values ] = rw_inheritable_props[prop];

			for (ds in datasets) {
				ds = datasets[ds];

				t.doesNotThrow(getval,
				    /*JSSTYLED*/
				    `can get ${prop} on ${ds.name}`);
				t.ok(values.includes(val),
				    /*JSSTYLED*/
				    `${val} is one of ${values}`);
			}

			let defval = root[prop];
			let localval = values[defval === values[0] ? 1 : 0]
			fs1[prop] = localval;
			t.equals(root[prop], defval,
				/*JSSTYLED*/
				`${prop}=${localval} on fs1 did not change ` +
				    `value on root`);
			t.equals(fs1[prop], localval,
				/*JSSTYLED*/
				`${prop}=${localval} on fs1 changed ` +
				    `value on fs1`);
			t.equals(fs2[prop], localval,
				/*JSSTYLED*/
				`${prop}=${localval} on fs1 changed ` +
				    `value on fs2`);
		}

		function setval() {
			ds[prop] = {};
		}

		for (prop in ro_props) {
			prop = ro_props[prop];

			for (ds in datasets) {
				ds = datasets[ds];
				t.doesNotThrow(getval,
				    /*JSSTYLED*/
				    `can get ro prop ${prop} on ${ds.name}`);
				check_verror(t, setval, 'ReadOnlyPropertyError',
				    /*JSSTYLED*/
				    `cannot set ro ${prop} on ${ds.name}`);
			}
		}

		t.end();
	});

	tt.test('mock-fs integration', function (t) {
		mockfs({ '/test123': {} });

		var top = new Dataset(null, 'test123', 'filesystem');
		t.equals(top.mounted, true, 'top-level dataset is mounted');

		var fs1 = new Dataset(top, 'fs1', 'filesystem');
		var file1 = '/test123/fs1/file1';
		var file1_content = 'file1 stuff';
		fs.writeFileSync(file1, file1_content, { mode: 0o644 });
		t.equals(fs.readFileSync(file1).toString(), file1_content,
			'file1 content can be read after writing');

		var snap1 = fs1.snapshot('snap1');
		fs.writeFileSync('/test123/fs1/file2', 'file2 stuff',
		    { mode: 0o644 });

		var fs2 = snap1.clone('test123/fs2');
		var content;
		t.doesNotThrow(function () {
			content = fs.readFileSync('/test123/fs2/file1')
			    .toString();
		}, 'can read /test123/fs2/file1');
		t.equals(content, file1_content,
			'/test123/fs2/file1 has expected content');

		var files;
		t.doesNotThrow(function () {
			files = fs.readdirSync(fs2.mountpoint);
		}, 'can readdir(' + fs2.mountpoint + ')');
		t.equals(files.length, 1, 'fs2 mountpoint has one file');
		t.equals(files[0], 'file1', 'fs2 contains file1');

		fs2.unmount();
		t.doesNotThrow(function () {
			files = fs.readdirSync(fs2.mountpoint);
		}, 'can readdir(' + fs2.mountpoint + ') after unmount');
		t.equals(files.length, 0,
			'fs2 mountpoint is empty after unmount');

		fs2.mount();
		t.doesNotThrow(function () {
			files = fs.readdirSync(fs2.mountpoint);
		}, 'can readdir(' + fs2.mountpoint + ')');
		t.equals(files.length, 1,
			'fs2 mountpoint has one file after mount');
		t.equals(files[0], 'file1', 'fs2 contains file1 after mount');
		t.doesNotThrow(function () {
			content = fs.readFileSync('/test123/fs2/file1')
			    .toString();
		}, 'can read /test123/fs2/file1 after mount');
		t.equals(content, file1_content,
			'/test123/fs2/file1 has expected content');

		// Set up a more complex directory hierarchy, then rename the
		// dataset. Be sure everything pops up in the right place under
		// the new name.
		fs.mkdirSync('/test123/fs2/dir1');
		fs.mkdirSync('/test123/fs2/dir1/dir2');
		fs.symlinkSync('/some/where', '/test123/fs2/dir1/link1');
		t.equals('/some/where',
		    fs.readlinkSync('/test123/fs2/dir1/link1'),
		    '/test123/fs2/dir1/link1 symlink points to /some/where');
		fs.writeFileSync('/test123/fs2/dir1/dir2/file3', 'file3 stuff');

		t.doesNotThrow(function () {
			fs2.rename('test123/fs2a');
		}, 'can rename a mounted file system that has stuff in it');
		var stat;
		t.throws(function () {
			fs.lstatSync('/test123/fs2/dir1');
		}, 'fs content no longer visible under old mountpoint');
		t.doesNotThrow(function () {
			stat = fs.lstatSync('/test123/fs2a');
		}, '/test123/fs2a exists');
		t.ok(stat.isDirectory(), '/test123/fs2a is a directory');
		t.doesNotThrow(function () {
			stat = fs.lstatSync('/test123/fs2a/dir1');
		}, '/test123/fs2a/dir1 exists');
		t.ok(stat.isDirectory(), '/test123/fs2a/dir1 is a directory');
		t.doesNotThrow(function () {
			stat = fs.lstatSync('/test123/fs2a/dir1/dir2');
		}, '/test123/fs2a/dir1/dir2 exists');
		t.ok(stat.isDirectory(),
			'/test123/fs2a/dir1/dir2 is a directory');
		t.doesNotThrow(function () {
			stat = fs.lstatSync('/test123/fs2a/dir1/link1');
		}, '/test123/fs2a/dir1/link1 exists');
		t.ok(stat.isSymbolicLink(),
			'/test123/fs2a/dir1/link1 is a symlink');
		t.equals('/some/where',
			fs.readlinkSync('/test123/fs2a/dir1/link1'),
			'symlink points to /some/where');
		t.doesNotThrow(function () {
			stat = fs.lstatSync('/test123/fs2a/dir1/dir2/file3');
		}, '/test123/fs2a/dir1/dir2/file3 exists');
		t.ok(stat.isFile(), '/test123/fs2a/dir1/dir2/file3 is a file');
		t.doesNotThrow(function () {
			content = fs
				.readFileSync('/test123/fs2a/dir1/dir2/file3')
				.toString();
		}, 'can read file3');
		t.equals(content, 'file3 stuff', 'file3 has the right stuff');

		t.end();
	});

	tt.test('catch new implementations', function (t) {
		// Nag anyone that implements these.
		let props = [
			'aclinherit',
			'acltype',
			'casesensitivity',
			'context',
			'dedup',
			'defcontext',
			'devices',
			'dnodesize',
			'encryption',
			'exec',
			'filesystem_count',
			'filesystem_limit',
			'fscontext',
			'keyformat',
			'keylocation',
			'logbias',
			'logicalreferenced',
			'logicalused',
			'mlslabel',
			'nmbmand',
			'normalization',
			'objsetid',
			'overlay',
			'pbkdf2iters',
			'primarycache',
			'quota',
			'readonly',
			'recordize',
			'redundant_metadata',
			'refcompressratio',
			'referenced',
			'refquota',
			'refreservation',
			'relatime',
			'reservation',
			'rootcontext',
			'secondarycache',
			'setuid',
			'sharenfs',
			'sharesmb',
			'size',
			'snapdev',
			'snapdir',
			'snapshot_count',
			'snapshot_limit',
			'special_small_blocks',
			'sync',
			'usedbychildren',
			'usedbydataset',
			'usedbyrefreservation',
			'usedbysnapshots',
			'utf8only',
			'version',
			'volblocksize',
			'volmode',
			'vscan',
			'written',
			'xattr',
			'zoned'
		];

		let root = new Dataset(null, 'root', 'filesystem');
		for (let prop in props) {
			prop = props[prop];
			try {
				root[prop] = 1;
				t.ok(false,
				    /*JSSTYLED*/
				    `unexpectedly able to set '${prop}'`);
			} catch (err) {
				t.equal(err.message, 'not implemented',
					`'set ${prop}' still not implemented`);
			}
			try {
				let foo = root[prop]
				t.ok(false,
				    `unexpectedly able to get ` +
				    /*JSSTYLED*/
				    `'${prop}' (${foo})`);
			} catch (err) {
				t.equal(err.message, 'not implemented',
					`'get ${prop}' still not implemented`);
			}
		}

		t.end();
	});

	tt.end();
});
