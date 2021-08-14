/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * This file provides the Dataset class and supporting routines required for
 * managing fake zpools and file system contents within fake filesystems.
 *
 * This is a low-level interface, mostly used by ./mock-zfs.js to mock node-zfs
 * (./zfs.js).  See tests for examples.
 *
 * If ./mock-zfs.js is not used, the typical usage would look like:
 *
 *     var mockfs = require('mock-fs');
 *     var mockds = require('mock-Dataset');
 *     var Dataset = mockds.Dataset;
 *
 *     mockfs({ '/testpool': {});
 *
 *     // creates a pool.  The top-level dataset is fake mounted at /testpool.
 *     var top = new Dataset(null, 'testpool', 'filesystem');
 *
 *     // create a file in the mocked file system.
 *     fs.writeFileSync('/testpool/file1', 'Hello World!\n')
 *
 *     // create a child filesystem, put some stuff in it, then snapshot it
 *     var fs1 = new Dataset(top, 'fs1', 'filesystem');
 *     fs.writeFileSync('/testpool/fs1/somefile', 'stuff');
 *     fs.mkdirSync('/testpool/fs1/somedir');
 *     fs.symlinkSync('/anywhere', '/testpool/fs1/somelink');
 *     var snap1 = fs1.snapshot('snap1')
 *
 *     // create a clone of testpool/fs1@snap1 and get the expected content
 *     var fs2 = snap1.clone('testpool/fs2');
 *     assert.equal(fs.readFileSync('/testpool/fs1/somefile').toString(),
 *                  fs.readFileSync('/testpool/fs2/somefile').toString())
 *
 * The operations above assume that the mountpoint and canmount properties cause
 * the filesystems to be mounted within a mock-fs filesystem.  Filesystems can
 * be mounted and unmounted with Dataset.mount() and Dataset.unmount().
 *
 * Other forms of fs mocking can be used to, so long as fs.stat().dev ===
 * 8675309.
 *
 * When a filesystem is unmounted or snapshotted, the content of that dataset is
 * archived and stored with the Dataset object for the filesystem or snapshot.
 * When a snapshot of a filesystem is cloned, the filesystem references the
 * snapshot's archive.  When a filesystem is mounted, the archived data is
 * restored to the mock filesystem and the archive is deleted (reference
 * removed, may still exist on origin snapshot.)
 */
const assert = require('assert-plus');
const deepcopy = require('deepcopy');
const fs = require('fs');
const path = require('path');
const sprintf = require('sprintf-js').sprintf;
const VError = require('verror').VError;

var fsver = 5;

// mock-fs sets st_dev to Jenny's phone number.  Filesystem changes only happen
// in mock file systems.
var mockfs_dev = 8675309;

// Used when setting creation_txg.  If pending_txg is 0, the Dataset constructor
// assumes that the caller is creating multiple datasets in the same txg.
var txg;
var pending_txg;

// The parent of all top-level datasets.  This needs to look like a dataset in
// some says, but isn't one.
var pools;

// Maps mountpoints to Dataset objects.
var mnttab;

/*
 * Blow away all the state to start a new test.
 */
function reset() {

	pools = {
		_children: {},
		// In `zfs get` output these are from the 'default' source.
		_local: {
			atime: 'on',
			canmount: 'on',
			checksum: 'on',
			compression: 'off',
			copies: 1,
			dedup: 'off',
			devices: 'on',
			encryption: 'off',
			exec: 'on',
			keyformat: 'none',
			keylocation: 'none',
			logbias: 'latency',
			mlslabel: 'none',
			mountpoint: '/',
			nbmand: 'off',
			normalization: 'none',
			overlay: 'off',
			primarycache: 'all',
			quota: 'none',
			readonly: 'off',
			recordsize: 128 * 1024,
			redundant_metadata: 'all',
			refquota: 'none',
			refreservation: 'none',
			relatime: 'off',
			reservation: 'none',
			secondarycache: 'all',
			setuid: 'on',
			sharenfs: 'off',
			sharesmb: 'off',
			snapdev: 'hidden',
			snapdir: 'hidden',
			sync: 'standard',
			version: 5,
			volmode: 'default',
			vscan: 'off',
			xattr: 'on',
			zoned: 'off'
		}
	};

	txg = 1;
	pending_txg = 0;
	mnttab = {};
}

/*
 * Given a dataset name or a Dataset, return the pool name.  If a name is
 * given, the return value says nothing about the existence of that pool.
 * @param {String|Dataset} ds - The name of a dataset or a dataset.
 * @return string - The name of a pool that may or may not exist.
 */
function getPoolname(ds) {
	if (typeof (ds) === 'string') {
		ds = ds.split(/[@\/]/)[0];
		return (ds);
	}
	while (ds._parent !== pools) {
		ds = ds._parent;
	}
	return (ds._name);
}

/*
 * Find a dataset by name.  For example, getDataset('data/foo@blah').
 * @returns {Dataset|null}
 */
function getDataset(fullname) {
	var name, snapname;
	[ name, snapname ] = fullname.split('@');
	var parts = name.split('/');
	var cur = pools;

	// Look up the filesystem or volume
	for (var i in parts) {
		name = parts[i];
		cur = cur._children[name];
		if (!cur) {
			return (null);
		}
	}

	if (snapname) {
		return (cur._snapshots[snapname]);
	}

	return (cur);
}

function getPools() {
	return (Object.keys(pools._children));
}

function destroyPool(poolname) {
	var pool = pools._children[poolname];
	if (!pool) {
		throw new VError({ info: poolname, name: 'NoSuchPoolError' },
			'pool \'%s\' does not exist, pool');
	}

	var datasets = [];
	for (var ds of pool.iterDescendants([ 'all' ])) {
		datasets.push(ds);
	}
	datasets.reverse();
	for (ds in datasets) {
		ds = datasets[ds];
		if (ds.mounted) {
			try {
				ds.unmount();
			} catch (_) {
				// nothing.
			}
		}
		ds._state = 'pool_destroyed';
	}

	delete pools._children[poolname];
}

function namecheck(name) {
	if (typeof (name) !== 'string') {
		throw new VError({ info: name, name: 'DatasetNameError' },
			'name must be a string');
	}
	// This probably doesn't do unicode and I'm ok with that.
	if (name.length === 0 || name.length > 255) {
		throw new VError({ info: name, name: 'DatasetNameError' },
			'name must be 1 to 255 characters long');
	}
	if (!name.match(/^[a-zA-Z0-9\-_\.: ]+$/)) {
		throw new VError({ info: name, name: 'DatasetNameError' },
		    'name may contain only letters numbers, - _ . : and space');
	}
}

/*
 * Is @fname in a mock-fs file system?
 * @param {string} fname - The name of the file or directory to check.
 * @returns boolean
 * @throws {Error} from fs.lstatSync() if @fname cannot be stat'd.
 */
function isMockFs(fname) {
	return (fs.lstatSync(fname).dev === mockfs_dev);
}

/*
 * If @fname exists or were to exist, would it be in a mocked filesystem?
 * @param {string} fname - The name of the file or directory to check.
 * @returns boolean
 */
function isUnderMockFs(fname) {
	assert.string(fname, 'fname');
	assert(fname.startsWith('/'), 'fname is an absolute path');

	while (fname !== '/') {
		try {
			return (isMockFs(fname));
		} catch (err) {
			if (err.code !== 'ENOENT') {
				throw err;
			}
		}
		fname = path.dirname(fname);
	}
	return (false);
}

/*
 * Is there anything mounted below this directory?
 * @param {string} dirname - The directory to check
 * @returns boolean
 */
function hasSubmounts(dirname) {
	assert.string(dirname, 'dirname');

	dirname = dirname + '/';
	for (var mntpt in mnttab) {
		if (mntpt.startsWith(dirname)) {
			return (true);
		}
	}

	return (false);
}

/*
 * Gather metadata (and data, if a file) of the specified path, storing it in an
 * object that can later be passed to restore().  If @name is a directory,
 * recursion will take place and the return value will include the directory's
 * content.
 * @param {string} name - the name of the file or directory to archive
 * @param {string} [newname] - The name of the object being archived.  If not
 * specified, it defaults to '.'.  Most callers will leave this unspecified.
 * @return {Object} - best not to intrept this with anything other than
 * restore().
 */
function archive(name, newname) {
	assert.string(name, 'name');
	assert(isMockFs(name), 'name is a mocked file system');

	var obj = {};
	obj.name = newname ? newname : '.';
	obj.lstat = fs.lstatSync(name);

	if (obj.lstat.isFile()) {
		assert(obj.lstat.size <= 1024 * 1024,
		    'file size limited to 1 MiB');
		obj.filedata = fs.readFileSync(name);
		return (obj);
	}
	if (obj.lstat.isSymbolicLink()) {
		obj.target = fs.readlinkSync(name);
		return (obj);
	}
	if (obj.lstat.isDirectory()) {
		obj.children = [];
		var kids = fs.readdirSync(name);
		for (var kid in kids) {
			kid = kids[kid];
			var kpath = path.join(name, kid);
			// Do not cross mount points.
			if (mnttab[kpath]) {
				continue;
			}
			obj.children.push(archive(kpath, kid));
		}
		return (obj);
	}

	throw new VError({
		info: {
			path: name,
			lstat: obj.lstat
		},
		name: 'InvalidFileTypeError'
	}, 'Cannot archive \'%s\': unsupported file type', name);
}

/*
 * Restore the object(s) archived with archive().
 * @param {string} basedir - directory into which @obj will be restored
 * @param {Object} obj - An object returned by archive().
 */
function restore(basedir, obj) {
	assert.string(basedir, 'basedir');
	assert(isMockFs(basedir), 'basedir is in a mock-fs');
	assert.object(obj, 'obj');
	assert.string(obj.name, 'obj.name');
	assert(obj.name.indexOf('/') === -1, 'obj.name contains no /');
	assert(obj.name.length > 0, 'obj.name is not a null string');
	assert.object(obj.lstat, 'obj.lstat');

	var name = path.join(basedir, obj.name);

	if (obj.lstat.isFile()) {
		fs.writeFileSync(name, obj.filedata, { mode: obj.lstat.mode });
	} else if (obj.lstat.isSymbolicLink()) {
		fs.symlinkSync(obj.target, name);
	} else if (obj.lstat.isDirectory()) {
		if (obj.name === '.') {
			// This is the top-level directory of a dataset.  It is
			// likely to exist.  Remove it so that it can be
			// recreated in the path most  traveled.
			name = basedir;
			try {
				stat = fs.rmdirSync(name);
			} catch (err) {
				if (err.code !== ENOENT) {
					throw err;
				}
			}
		}
		fs.mkdirSync(name, 0o777);
		for (var child in obj.children) {
			restore(name, obj.children[child]);
		}
		fs.chmodSync(name, obj.lstat.mode);
	} else {
		throw new VError({
			info: {
				path: name,
				lstat: obj.lstat
			},
			name: 'InvalidFileTypeError'
		}, 'Cannot restore \'%s\': unsupported file type', name);
	}

	fs.utimesSync(name, obj.lstat.atimeMs / 1000, obj.lstat.mtimeMs / 1000);
}


/*
 * Recursive delete, taking special care to not cross mount points.
 * @param {string} dir - Directory that will be emptied
 */
function clearDir(dir) {
	assert.string(dir, 'dir');
	assert(isMockFs(dir), 'removing files from mocked fs');

	var children = fs.readdirSync(dir);
	for (var child in children) {
		child = children[child];
		var cpath = path.join(dir, child);

		assert(!mnttab[cpath],
			'child path \'' + cpath + '\' is not a mountpoint');

		if (fs.lstatSync(cpath).isDirectory()) {
			clearDir(cpath);
			fs.rmdirSync(cpath);
			continue;
		}
		fs.unlinkSync(cpath);
	}
}

function parseHumanNumber(hnum) {
	assert.string(hnum);

	var parts = hnum.match(/^(\d+)([bkmgtpe]?)$/i);
	if (!parts) {
		throw new VError({ name: BadHumanNumberError, info: hnum },
			'cannot parse number \'%s\'', hnum);
	}
	var base = parseInt(parts[1]);
	var scale = parts[2].toLowerCase()
	if (scale === '') {
		return (base);
	}
	var exp = [ 'b', 'k', 'm', 'g', 't', 'p', 'e' ].indexOf(scale);
	assert(exp !== -1, 'hnum parser weeded out bad scales');
	return (base * Math.pow(2, exp * 10));
}

class Dataset {
	/*
	 * Create a new dataset (filesystem, volume, or snapshot).
	 * @param {Dataset|null} parent - The parent Dataset or null if creating
	 * a new pool.
	 * @param {string} name - The name of the new dataset, relative to
	 * parent.name.  That is, name must not have '/' or '@' in it.
	 * @param {string} type - One of 'filesystem', 'volume', or 'snapshot'.
	 * @param {Object} [properties] - Optional ZFS propeties to set on this
	 * dataset.
	 * @param {Object} [fscontent] - Optional filesystem content
	 */
	constructor(parent, name, type, props, fscontent) {
		var self = this;
		props = props || {};

		self._parent = parent || pools;
		namecheck(name);

		if (self._parent === pools && type !== 'filesystem') {
			throw new VError({
				info: arguments,
				name: 'DatasetTypeError'
			}, 'top level dataset must be a filesystem');
		}

		self._name = name;
		self._local = {
			type: type,
			creation: new Date(),
			createtxg: txg,
			version: props.version || fsver,
			guid: Math.floor(Math.random() * Math.pow(2, 64))
		};
		self._mounted = false;
		self._fscontent = fscontent ? fscontent : null;

		var siblings;
		switch (type) {
			case 'filesystem':
				self._sep = '/';
				self._parent_types = new Set([ 'filesystem' ]);
				self._child_types = new Set([ 'snapshot',
				    'volume' ]);
				self._children = {};
				self._snapshots = {};
				siblings = self._parent._children;
				break;
			case 'volume':
				self._sep = '/';
				self._parent_types = new Set([ 'filesystem' ]);
				self._child_types = new Set([ 'snapshot' ]);
				self._snapshots = {};
				self._local.volblocksize = 8192;
				siblings = self._parent._children;
				break;
			case 'snapshot':
				self._sep = '@';
				self._parent_types = new Set([ 'filesystem',
				    'volume' ]);
				self._child_types = new Set();
				self._holds = new Set();
				self._clones = [];
				siblings = self._parent._snapshots;
				break;
			default:
				throw new VError({
					info: arguments,
					name: 'DatasetTypeError'
				}, `unsupported dataset type '${type}'`);
		}

		/*
		 * Set props that were passed in via the setter.  A setter that
		 * allows special behavior during creation (e.g. setting
		 * volblocksize, encryption) should check self._creating.  Once
		 * creation is done, we seal this object so that a caller cannot
		 * accidentally set a property that this not supported.
		 */
		self._state = 'creating';
		for (var prop in props) {
			self[prop] = props[prop];
		}
		Object.seal(self);
		// A little premature, but allows getters to work from here on.
		self._state = 'active';

		if (self._parent !== pools &&
			!self._parent_types.has(self._parent.type)) {

			throw new VError({
				info: arguments,
				name: 'DatasetTypeError'
			}, 'type %j must be in %j', type, self._parent_types);
		}

		if (siblings.hasOwnProperty(name)) {
			throw new VError({
				info: {
					parent: self._parent,
					newname: name,
					newtype: type
				},
				name: 'DatasetExistsError'
			}, `'${self.name}' already exists`);
		}

		siblings[name] = self;

		if (pending_txg == 0) {
			txg++;
		}

		if (type === 'filesystem' && self.canmount === 'on') {
			self.mount({ ignore_not_mountable: true });
		}
	}

	_assertActive() {
		var self = this;

		if ([ 'active', 'creating' ].indexOf(self._state) === -1) {
			throw new VError({
				name: 'InactiveDatasetError',
				info: self
			}, 'dataset state is \'%s\', not \'active\'',
				self._state);
		}
	}

	/*
	 * Iterate over the children (filesystem, volume, snapshot) and/or
	 * dependents (clones of snapshots) of a dataset.
	 * @param {(string[]|Set)} types - The types of datasets to iterate.
	 *     Valid types are 'filesystem', 'volume', 'snapshot', and 'clones'.
	 *     'all' implies 'filesystem', 'volume', and 'snapshot', as in
	 *     `zfs list -r -t all`.  If 'clones' is included, this becomes more
	 *     like `zfs list -R`
	 * @return {Dataset} - Each next() returns a dataset
	 */
	* iterDescendants(types, state) {
		this._assertActive();
		var self = this;
		state = state || {};
		state.visited = state.visited || new Set();
		types = new Set(types);
		var do_fs = types.has('all') || types.has('filesystem');
		var do_vol = types.has('all') || types.has('volume');
		var do_snap = types.has('all') || types.has('snapshot');
		var do_clones = types.has('clones');
		var child;

		const oktypes = new Set([
			'all', 'filesystem', 'volume', 'snapshot', 'clones' ]);
		for (var type of types) {
			if (!oktypes.has(type)) {
				throw new VError({
					name: 'InvalidArgumentError'
				}, `type '${type}' is not valid`);
			}
		}

		if (!do_fs && !do_vol && !do_snap) {
			throw new VError({
				info: arguments,
				name: 'InvalidArgumentError'
			}, 'iterDescendants() requires dataset type');
		}

		// With 'clones' duplicates are possible if not careful.
		if (state.visited.has(self)) {
			return;
		}
		state.visited.add(self);

		if (types.has('all') || types.has(self.type)) {
			yield self;
		}

		// List snapshots and clones
		if (do_snap || do_clones) {
			for (child in self._snapshots) {
				child = self._snapshots[child];
				yield * child.iterDescendants(types, state);
				if (do_clones) {
					for (var clone in child._clones) {
						clone = child._clones[clone];
						yield * clone.iterDescendants(
						    types, state);
					}
				}
			}
		}

		// List child filesystems, volumes, and their snapshots.
		for (child in self._children) {
			yield * self._children[child].iterDescendants(types,
			    state);
		}
	}

	/*
	 * Iterate over descendants calling checkcb() on each, then docb() on
	 * each.  If filtercb is specified, it can be used to reduce those that
	 * are checked and done.
	 * @param {function} checkcb - Called as checkcb(Dataset).  It should
	 *     throw an error to interrupt iteration if it is not happy with a
	 *     dataset.
	 * @param {function} docb - Called as docb(Dataset).
	 * @param {function} [filtercb] - If present, it should return false for
	 * those datasets that should not be checked or done.
	 */
	_doDescendants(types, checkcb, docb, filtercb) {
		assert.arrayOfString(types, 'types');
		assert.func(checkcb, 'checkcb');
		assert.func(docb, 'docb');
		assert.optionalFunc(filtercb, 'filtercb');
		var self = this;
		var ds;

		for (ds of self.iterDescendants(types)) {
			if (filtercb && !filtercb(ds)) {
				continue;
			}
			checkcb(ds);
		}
		for (ds of self.iterDescendants(types)) {
			if (filtercb && !filtercb(ds)) {
				continue;
			}
			docb(ds);
		}
	}

	mount(opts) {
		var self = this;

		self._assertActive();
		assert.optionalObject(opts, 'opts');
		opts = opts || {};
		assert.optionalBool(opts.ignore_not_mountable,
			'opts.ignore_not_mountable');
		var ignore = opts.ignore_not_mountable;

		function not_mountable(reason) {
			if (ignore) {
				return;
			}
			throw new VError({
				info: self,
				name: 'UnmountableError'
			}, 'cannot mount \'%s\' on \'%s\': %s',
			    self.name, self.mountpoint, reason);
		}

		if (self.type !== 'filesystem') {
			return (not_mountable('type=' + self.type));
		}
		if (self.mounted) {
			return (not_mountable('already mounted'));
		}
		if (self.canmount === 'off') {
			return (not_mountable('canmount=off'));
		}
		if (!self.mountpoint.startsWith('/')) {
			return (not_mountable('mountpoint is not an absolute ' +
			    'path'));
		}
		if (!isUnderMockFs(self.mountpoint)) {
			return (not_mountable('mountpoint is not in roots ' +
			    'array'));
		}

		try {
			if (fs.readdirSync(self.mountpoint).length !== 0) {
				throw new VError({
					info: self,
					name: 'OverlayMountError'
				}, 'cannot mount \'%s\' on \'%s\': ' +
				    'directory not empty',
				    self.name, self.mountpoint);
			}
		} catch (err) {
			if (err.code !== 'ENOENT') {
				throw err;
			}
			fs.mkdirSync(self.mountpoint, 0o755);
		}

		mnttab[self.mountpoint] = self;
		self._mounted = true;
		if (self._fscontent) {
			restore(self.mountpoint, self._fscontent);
			self._fscontent = null;
		}
	}

	unmount() {
		var self = this;

		self._assertActive();

		if (!self.mounted) {
			return;
		}

		if (hasSubmounts(self.mountpoint)) {
			throw new VError({
				info: self,
				name: 'FilesystemBusyError'
			}, 'cannot unmount \'%s\': has submounts', self.name);
		}

		self._fscontent = archive(self.mountpoint)
		// Note that this does not rmdir the mountpoint.  While this
		// may pollute a parent dataset differently than zfs really does
		// (that behavior is undocumented), it is important to avoid
		// removing a top-level mock filesystem.
		clearDir(self.mountpoint);
		delete mnttab[self.mountpoint];
		self._mounted = false;
	}

	/*
	 * Destroy this dataset, and perhaps its decsendants.
	 * @param {Object} opts
	 * @param {boolean} opts.recursive - Destroy descendants (filesystems,
	 * volumes, and snapshots living below this dataset in the namespace).
	 * That is, `zfs destroy -r`, not `zfs destroy -R`.
	 */
	destroy(opts) {
		this._assertActive();
		var self = this;
		opts = opts || {};
		var recursive = opts.recursive || false;
		var clones = [];

		function check_destroy(check) {
			if (check.type === 'snapshot' &&
			    check._holds.size !== 0) {
				throw new VError({
					info: this,
					name: 'SnapshotHoldError'
				}, `snapshot '${check.name}' should ` +
					    `have no holds`);
			}
			var kids = check._children;
			var snaps = check._snapshots;
			if (!recursive && ((kids &&
			    Object.keys(kids).length !== 0) ||
				(snaps && Object.keys(snaps).length !== 0))) {

				throw new VError({
					info: this,
					name: 'DescendantError'
				}, `dataset '${check.name}' should have ` +
					`no children`);
			}

			for (var clone in check._clones) {
				clones.push(check._clones[clone]);
			}
		}

		// Gather dataset list and sanity check.
		check_destroy(self);
		var todestroy = [];
		if (recursive) {
			for (ds of this.iterDescendants(['all'])) {
				if (ds === self) {
					continue;
				}
				check_destroy(ds);
				todestroy.push(ds);
			}
		}

		for (var ds in clones) {
			ds = clones[ds];

			if (todestroy.indexOf(ds) === -1) {
				throw new VError({
					info: this,
					opts: opts,
					name: 'DependantError',
					dataset: ds
				}, `dataset '${ds.name}' requires origin ` +
				    `snapshot '${ds.origin.name}' which ` +
				    `would be deleted`);
			}
		}

		// Destroy in reverse order from iteration.  That is, destroy
		// children first.
		while (todestroy.length > 0) {
			todestroy.pop().destroy({recursive: false})
		}

		self.unmount();

		if (self.type === 'snapshot') {
			delete self._parent._snapshots[self._name];
		} else {
			if (self._local.origin) {
				clones = self._local.origin._clones;
				clones.splice(clones.indexOf(self), 1);
			}
			delete self._parent._children[self._name];
		}

		self._state = 'destroyed';
	}

	/*
	 * Create a snapshot of a filesystem or hierarchy
	 * @param {string} snapname - the name of the snapshot
	 * @param {Object} [opts]
	 * @param {boolean} [opts.recursive] - if true, be like
	 *     `zfs snapshot -r`.
	 * @param {Object} [properties] - Few properties should be settable on
	 * snapshots.  This is probably most useful for user properties.
	 * @return {Dataset} the new snapshot dataset
	 */
	snapshot(snapname, opts, properties) {
		this._assertActive();
		var self = this;
		assert.optionalObject(opts);
		assert.optionalObject(properties);
		opts = opts || {};
		assert.optionalBool(opts.snapshot);
		properties = properties || {};
		var recursive = opts.recursive || false;
		var errinfo = {
			dataset: self,
			snapname: snapname,
			opts: opts,
			properties: properties
		};
		var newds;

		if (!self._child_types.has('snapshot')) {
			throw new VError({
					info: errinfo,
					name: 'DatasetTypeError'
				}, `cannot create snapshot of ` +
				/*JSSTYLED*/
				`${self.type} '${self.name}'`);
		}

		function checksnap(ds) {
			if (ds._snapshots.hasOwnProperty(snapname)) {
				throw new VError({
					info: errinfo,
					name: 'DatasetExistsError'
				}, `'${ds.name}@${snapname}' already exists`);
			}
		}

		function dosnap(ds) {
			newds = new Dataset(ds, snapname, 'snapshot',
			    properties);
			ds._snapshots[snapname] = newds;
			newds._fscontent = ds._fscontent;
		}

		pending_txg = txg;

		try {
			if (recursive) {
				self._doDescendants(['filesystem', 'volume'],
				    checksnap, dosnap);
			} else {
				checksnap(self);
				dosnap(self);
			}
			// XXX need except?
		} finally {
			txg++;
			pending_txg = 0;
		}

		newds = self._snapshots[snapname];

		if (self._fscontent) {
			// The filesystem was previously mounted, populated,
			// then unmounted. Or it could be a clone of a
			// filesystem that did the same.
			newds._fscontent = self._fscontent;
		} else if (self._parent._mounted) {
			newds._fscontent = archive(self.mountpoint);
		}

		return (newds);
	}

	clone(newname, opts, properties) {
		this._assertActive();
		var self = this;
		var myname = self.name;
		var poolname = getPoolname(self);
		opts = opts || {};
		properties = properties || {};
		var parents = opts.parents || false;

		if (self.type !== 'snapshot') {
			throw new VError({
				info: {
					dataset: self,
					args: arguments
				},
				name: 'DatasetTypeError'
			}, 'can only clone snapshots');
		}

		if (poolname !== getPoolname(newname)) {
			throw new VError({
				info: {
					dataset: self,
					args: arguments
				},
				name: 'InvalidArgumentError'
			}, `snapshot '${self.name}' and '${newname}' not in ` +
			    `same pool`);
		}
		assert(!newname.startsWith(myname.split('@')[0] = '/'));

		var pname = path.dirname(newname);
		var pds = getDataset(pname);
		if (!pds) {
			if (!opts.parents) {
				throw new VError({
					info: {
						dataset: self,
						parent_name: pname,
						opts: opts,
						properties: properties
					},
					name: 'InvalidArgumentError'
				}, 'parent of \'%s\' must exist', newname);
			}
			var tocreate = [];
			while (!pds && pname !== poolname) {
				pds = getDataset(pname)
				tocreate.push(pname);
				pname = path.dirname(pname);
			}
			if (!pds && pname === poolname) {
				pds = getDataset(pname);
			}
			assert(pds, 'a parent must exist');
			pname = tocreate.pop();
			while (pname) {
				pds = new Dataset(pds, path.basename(pname),
				    'filesystem');
				assert(pds, 'a child dataste was created');
				pname = tocreate.pop();
			}
		}

		var newds = new Dataset(pds, path.basename(newname),
		    self._parent.type, properties, self._fscontent);
		newds._local.origin = self;
		self._clones.push(newds);

		return (newds);
	}

	hold(reason, opts) {
		this._assertActive();
		var self = this;
		opts = opts || {};
		var recursive = opts.recursive;
		var child, childsnap;

		if (self.type !== 'snapshot') {
			throw new VError({
				info: {
					dataset: self,
					args: arguments
				},
				name: 'DatasetTypeError'
			}, 'can only clone snapshots');
		}

		assert(self.type === 'snapshot');
		assert(!self._holds.has(reason));

		function checkhold(_ds) { }

		function addhold(ds) {
			ds._holds.add(reason);
		}

		function filter(ds) {
			return (ds._name === self._name);
		}

		if (recursive) {
			self._parent._doDescendants(['snapshot'], checkhold,
			    addhold, filter);
			return;
		}

		addhold(self);
	}

	release(reason, opts) {
		this._assertActive();
		var self = this;
		opts = opts || {};
		var recursive = opts.recursive;
		var child, childsnap;

		if (self.type !== 'snapshot') {
			throw new VError({
				info: {
					dataset: self,
					args: arguments
				},
				name: 'DatasetTypeError'
			}, 'can only clone snapshots');
		}

		assert(self.type === 'snapshot');
		assert(self._holds.has(reason),
		    /*JSSTYLED*/
		    `release ${reason} from ${self.name}`);

		function checkhold(_ds) { }

		function rmhold(ds) {
			ds._holds.delete(reason);
		}

		function filter(ds) {
			return (ds._name === self._name);
		}

		if (recursive) {
			self._parent._doDescendants(['snapshot'], checkhold,
				rmhold, filter);
			return;
		}

		rmhold(self);
	}

	holds() {
		this._assertActive();
		var self = this;

		if (self.type !== 'snapshot') {
			throw new VError({
				info: {
					dataset: self,
					args: arguments
				},
				name: 'DatasetTypeError'
			}, 'can only clone snapshots');
		}

		return (new Set(self._holds));
	}

	rename(newname, opts) {
		this._assertActive();
		var self = this;
		opts = opts || {};
		var parents = opts.parents || false;
		var name, snapname, parentname;
		var pds;
		var errinfo = {
			dataset: self,
			newname: snapname,
			opts: opts
		};
		var mounted = self.mounted;

		if (getDataset(newname)) {
			throw new VError({
				info: errinfo,
				name: 'DatasetExistsError'
			}, 'cannot rename \'%s\': \'%s\' already exists',
			    self.name, newname);
		}

		[ name, snapname ] = newname.split('@');
		if (snapname) {
			if (self.type !== 'snapshot') {
				throw new VError({
					info: errinfo,
					name: 'InvalidArgumentError'
				}, 'cannot rename a %s to a snapshot',
				    self.type);
			}
			if (name !== self._parent.name) {
				throw new VError({
					info: errinfo,
					name: 'InvalidArgumentError'
				}, 'cannot rename a snapshot to a different ' +
				'parent');
			}
			parentname = name;
			pds = self._parent;
			assert.equal(pds._snapshots[self._name], self,
				'snapshot exists as old name');
			assert(!pds._snapshots[snapname],
				'snapshot does not exist with new name');
			pds._snapshots[snapname] = self;
			delete pds._snapshots[self._name];
			return;
		}

		/* Not a snapshot */

		if (self.type === 'snapshot') {
			throw new VError({
				info: errinfo,
				name: 'InvalidArgumentError'
			}, 'cannot rename a snapshot to a filesystem or volume',
			    self.type);
		}
		if (newname.search('/') === -1) {
			throw new VError({
				info: errinfo,
				name: 'InvalidArgumentError'
			}, 'new name cannot be a pool name');
		}
		if (getPoolname(self) !== getPoolname(newname)) {
			throw new VError({
				info: errinfo,
				name: 'InvalidArgumentError'
			}, 'cannot rename \'%s\': new name must be in same ' +
			    'pool', self.name);
		}

		assert(!parents, 'opts.parents not implemented');
		pds = getDataset(path.dirname(newname));

		assert(self._parent._children[self._name] === self,
			'self is a child of parent');
		assert(!pds._children[path.basename(newname)],
		    'newname is free');

		if (mounted) {
			self.unmount();
		}

		delete self._parent._children[self._name];
		self._parent = pds;
		self._name = path.basename(newname);
		pds._children[self._name] = self;

		if (mounted) {
			self.mount();
		}
	}

	/*
	 * getters, setters, and their helpers
	 */

	getInheritableValue(propname) {
		this._assertActive();
		var self = this;
		var source;

		var ds = self;
		while (!ds._local.hasOwnProperty(propname)) {
			assert(ds !== pools,
				`pools top-level object should have default ` +
				/*JSSTYLED*/
				`for ${propname}`)
			ds = ds._parent;
		}
		switch (ds) {
			case self:
				source = 'local';
				break;
			case pools:
				source = 'default';
				break;
			default:
				/*JSSTYLED*/
				source = `inherited from ${ds.name}`;
		}

		return {
			value: ds._local[propname],
			source: source
		};
	}

	get aclinherit() { throw new Error('not implemented'); }
	set aclinherit(_) { throw new Error('not implemented'); }
	get acltype() { throw new Error('not implemented'); }
	set acltype(_) { throw new Error('not implemented'); }

	get atime() {
		this._assertActive();
		return (this.getInheritableValue('atime').value);
	}

	set atime(value) {
		this._assertActive();
		assert([ 'on', 'off' ].indexOf(value) !== -1,
		    'atime is on or off');

		this._local.atime = value;
	}

	get canmount() {
		this._assertActive();
		return (this.getInheritableValue('canmount').value);
	}

	set canmount(value) {
		this._assertActive();
		assert([ 'on', 'off', 'noauto' ].indexOf(value) !== -1,
			'canmount is on, off, or noauto');

		this._local.canmount = value;
	}

	get casesensitivity() { throw new Error('not implemented'); }
	set casesensitivity(_) { throw new Error('not implemented'); }

	get checksum() {
		this._assertActive();
		return (this.getInheritableValue('checksum').value);
	}

	set checksum(value) {
		this._assertActive();
		var valid = [ 'on', 'off', 'fletcher2', 'fletcher4', 'sha256',
			'noparity', 'sha512', 'skein', 'edonr' ];
		assert(valid.indexOf(value) !== -1,
			sprintf('checksum is one of: %j', valid));

		// XXX:
		// The sha512, skein, and edonr checksum algorithms require
		// enabling the appropriate features on the pool.  These pool
		// features are not supported by GRUB and must not be used on
		// the pool if GRUB needs to access the pool (e.g. for /boot).

		this._local.checksum = value;
	}

	get compression() {
		this._assertActive();
		return (this.getInheritableValue('compression').value);
	}

	set compression(value) {
		this._assertActive();
		assert([ 'on', 'off' ].indexOf(value) !== -1,
			'compression is on or off');

		this._local.compression = value;
	}

	get context() { throw new Error('not implemented'); }
	set context(_) { throw new Error('not implemented'); }

	get copies() {
		this._assertActive();
		return (this.getInheritableValue('copies').value);
	}

	set copies(value) {
		this._assertActive();
		assert(Math.floor(parseInt(value)) === value,
			'copies must be an integer');
		assert(value >= 1 && value <= 3, 'copies must be 1, 2, or 3');

		this._local.copies = value;
	}

	get createtxg() {
		this._assertActive();
		return (this._local.createtxg);
	}

	set createtxg(_) {
		this._assertActive();
		throw new VError({
			name: 'ReadOnlyPropertyError',
			info: {
				dataset: this,
				property: 'createtxg'
			},
		}, 'property is read-only');
	}

	get creation() {
		this._assertActive();
		return (Math.floor(this._local.creation / 1000));
	}

	set creation(_) {
		this._assertActive();
		throw new VError({
			name: 'ReadOnlyPropertyError',
			info: {
				dataset: this,
				property: 'creation'
			},
		}, 'property is read-only');
	}

	get dedup() { throw new Error('not implemented'); }
	set dedup(_) { throw new Error('not implemented'); }
	get defcontext() { throw new Error('not implemented'); }
	set defcontext(_) { throw new Error('not implemented'); }
	get devices() { throw new Error('not implemented'); }
	set devices(_) { throw new Error('not implemented'); }
	get dnodesize() { throw new Error('not implemented'); }
	set dnodesize(_) { throw new Error('not implemented'); }
	get encryption() { throw new Error('not implemented'); }
	set encryption(_) { throw new Error('not implemented'); }
	get exec() { throw new Error('not implemented'); }
	set exec(_) { throw new Error('not implemented'); }
	get filesystem_count() { throw new Error('not implemented'); }
	set filesystem_count(_) { throw new Error('not implemented'); }
	get filesystem_limit() { throw new Error('not implemented'); }
	set filesystem_limit(_) { throw new Error('not implemented'); }
	get fscontext() { throw new Error('not implemented'); }
	set fscontext(_) { throw new Error('not implemented'); }

	get guid() {
		this._assertActive();
		return (this._local.guid);
	}

	set guid(val) {
		this._assertActive();
		throw new VError({
			name: 'ReadOnlyPropertyError',
			info: {
				dataset: this,
				property: 'guid'
			},
		}, 'property is read-only');
	}

	get keyformat() { throw new Error('not implemented'); }
	set keyformat(_) { throw new Error('not implemented'); }
	get keylocation() { throw new Error('not implemented'); }
	set keylocation(_) { throw new Error('not implemented'); }
	get logbias() { throw new Error('not implemented'); }
	set logbias(_) { throw new Error('not implemented'); }
	get logicalreferenced() { throw new Error('not implemented'); }
	set logicalreferenced(_) { throw new Error('not implemented'); }
	get logicalused() { throw new Error('not implemented'); }
	set logicalused(_) { throw new Error('not implemented'); }
	get mlslabel() { throw new Error('not implemented'); }
	set mlslabel(_) { throw new Error('not implemented'); }

	get mounted() {
		return (this._mounted);
	}

	set mounted(_) {
		this._assertActive();
		throw new VError({
			name: 'ReadOnlyPropertyError',
			info: {
				dataset: this,
				property: 'mounted'
			},
		}, 'property is read-only');
	}

	// XXX This should be made more generic to work 'zfs get' to support
	// 'inherited from foo/bar'.
	get mountpoint() {
		this._assertActive();
		var self = this;
		if (self.type !== 'filesystem') {
			return null
		}
		var cur = self;
		var trail = [];

		while (cur !== pools && !cur._local.mountpoint) {
			trail.push(cur._name);
			cur = cur._parent;
		}
		if (cur === pools) {
			return ('/' + trail.reverse().join('/'));
		}
		if (cur._local.mountpoint.startsWith('/')) {
			return ([ cur._local.mountpoint ]
			    .concat(trail.reverse()).join('/'));
		}
		return (cur._local.mountpoint);
	}

	set mountpoint(value) {
		this._assertActive();
		var self = this;

		// XXX snapshots of filesystems too?
		assert(self.type === 'filesystem',
			'mountpoint only supported with filesystems');

		assert(value.startsWith('/') ||
			[ 'none', 'legacy'].indexOf(value) !== -1,
			'mountpoint must be \'none\' or \'legacy\' or an ' +
			'absolute path');
		self.unmount();
		self._local.mountpoint = value;
		// XXX ignore_not_mountable is questionable here.
		self.mount({ ignore_not_mountable: true });
	}

	get nmbmand() { throw new Error('not implemented'); }
	set nmbmand(_) { throw new Error('not implemented'); }
	get normalization() { throw new Error('not implemented'); }
	set normalization(_) { throw new Error('not implemented'); }
	get objsetid() { throw new Error('not implemented'); }
	set objsetid(_) { throw new Error('not implemented'); }

	get origin() {
		this._assertActive();
		return (this._local.origin);
	}

	set origin(_) {
		this._assertActive();
		throw new VError({
			name: 'ReadOnlyPropertyError',
			info: {
				dataset: this,
				property: 'origin'
			},
		}, 'property is read-only');
	}

	get overlay() { throw new Error('not implemented'); }
	set overlay(_) { throw new Error('not implemented'); }
	get pbkdf2iters() { throw new Error('not implemented'); }
	set pbkdf2iters(_) { throw new Error('not implemented'); }
	get primarycache() { throw new Error('not implemented'); }
	set primarycache(_) { throw new Error('not implemented'); }

	get quota() {
		return (this.getInheritableValue('quota').value);
	}

	set quota(value) {
		var self = this;

		if (self.type !== 'filesystem') {
			throw new VError({
				name: 'UnsupportedPropertyError',
				info: {
					dataset: this,
					property: 'quota'
				},
			}, '\'quota\' is only supported on filesystems');
		}

		if (value === 'none') {
			delete this._local.quota
			return;
		}

		// XXX should be a multiple of 2^ashift (I think)
		this._local.quota = parseHumanNumber(value);
	}

	get readonly() { throw new Error('not implemented'); }
	set readonly(_) { throw new Error('not implemented'); }
	get recordize() { throw new Error('not implemented'); }
	set recordize(_) { throw new Error('not implemented'); }
	get redundant_metadata() { throw new Error('not implemented'); }
	set redundant_metadata(_) { throw new Error('not implemented'); }
	get refcompressratio() { throw new Error('not implemented'); }
	set refcompressratio(_) { throw new Error('not implemented'); }
	get referenced() { throw new Error('not implemented'); }
	set referenced(_) { throw new Error('not implemented'); }
	get refquota() { throw new Error('not implemented'); }
	set refquota(_) { throw new Error('not implemented'); }
	get refreservation() { throw new Error('not implemented'); }
	set refreservation(_) { throw new Error('not implemented'); }
	get relatime() { throw new Error('not implemented'); }
	set relatime(_) { throw new Error('not implemented'); }
	get reservation() { throw new Error('not implemented'); }
	set reservation(_) { throw new Error('not implemented'); }
	get rootcontext() { throw new Error('not implemented'); }
	set rootcontext(_) { throw new Error('not implemented'); }
	get secondarycache() { throw new Error('not implemented'); }
	set secondarycache(_) { throw new Error('not implemented'); }
	get setuid() { throw new Error('not implemented'); }
	set setuid(_) { throw new Error('not implemented'); }
	get size() { throw new Error('not implemented'); }
	set size(_) { throw new Error('not implemented'); }
	get sharenfs() { throw new Error('not implemented'); }
	set sharenfs(_) { throw new Error('not implemented'); }
	get sharesmb() { throw new Error('not implemented'); }
	set sharesmb(_) { throw new Error('not implemented'); }
	get snapdev() { throw new Error('not implemented'); }
	set snapdev(_) { throw new Error('not implemented'); }
	get snapdir() { throw new Error('not implemented'); }
	set snapdir(_) { throw new Error('not implemented'); }
	get snapshot_count() { throw new Error('not implemented'); }
	set snapshot_count(_) { throw new Error('not implemented'); }
	get snapshot_limit() { throw new Error('not implemented'); }
	set snapshot_limit(_) { throw new Error('not implemented'); }
	get special_small_blocks() { throw new Error('not implemented'); }
	set special_small_blocks(_) { throw new Error('not implemented'); }
	get sync() { throw new Error('not implemented'); }
	set sync(_) { throw new Error('not implemented'); }

	get type() {
		this._assertActive();
		return (this._local.type);
	}

	set type(_) {
		this._assertActive();
		throw new VError({
			name: 'ReadOnlyPropertyError',
			info: {
				dataset: this,
				property: 'type'
			},
		}, 'property is read-only');
	}


	get usedbychildren() { throw new Error('not implemented'); }
	set usedbychildren(_) { throw new Error('not implemented'); }
	get usedbydataset() { throw new Error('not implemented'); }
	set usedbydataset(_) { throw new Error('not implemented'); }
	get usedbyrefreservation() { throw new Error('not implemented'); }
	set usedbyrefreservation(_) { throw new Error('not implemented'); }
	get usedbysnapshots() { throw new Error('not implemented'); }
	set usedbysnapshots(_) { throw new Error('not implemented'); }
	get utf8only() { throw new Error('not implemented'); }
	set utf8only(_) { throw new Error('not implemented'); }
	get version() { throw new Error('not implemented'); }
	set version(_) { throw new Error('not implemented'); }
	get volblocksize() { throw new Error('not implemented'); }
	set volblocksize(_) { throw new Error('not implemented'); }
	get volmode() { throw new Error('not implemented'); }
	set volmode(_) { throw new Error('not implemented'); }
	get vscan() { throw new Error('not implemented'); }
	set vscan(_) { throw new Error('not implemented'); }
	get written() { throw new Error('not implemented'); }
	set written(_) { throw new Error('not implemented'); }
	get xattr() { throw new Error('not implemented'); }
	set xattr(_) { throw new Error('not implemented'); }
	get zoned() { throw new Error('not implemented'); }
	set zoned(_) { throw new Error('not implemented'); }

	get name() {
		this._assertActive();
		if (this._parent !== pools) {
			return (path.join(this._parent.name + this._sep +
			    this._name));
		}
		return (this._name);
	}

	set name(val) {
		this._assertActive();
		throw new VError({
			name: 'ReadOnlyPropertyError',
			info: {
				dataset: this,
				property: 'name'
			},
		}, 'property is read-only');
	}
}

reset();

module.exports = {
	Dataset: Dataset,
	destroyPool: destroyPool,
	get: getDataset,
	getPools: getPools,
	poolname: getPoolname,
	reset: reset
};
