/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * This mocks the behavior of node-zfs.  Hook it up with mockery or similar to
 * do unit tests that invoke zpool and zfs methods.
 */

const mockds = require('./mock-dataset.js');
const assert = require('assert-plus');
const sprintf = require('sprintf-js').sprintf;
const path = require('path');

const Dataset = mockds.Dataset;

var fs = {};

function zfsError(_error, stderr) {
    return new Error(stderr);
}

function notImplemented(what, detail) {
    if (!detail) {
        detail = 'not implemented';
    }
    return new Error('mockzfs.' + what + '(): ' + detail);
}

var zpool = exports.zpool = function () { };

zpool.listFields_ = [ 'name', 'size', 'allocated', 'free', 'cap',
    'health', 'altroot' ];

zpool.listDisks = function (callback) {
    callback(notImplemented('zpool.listDisks'));
};

zpool.list = function () {
    var pool, opts = {}, callback;
    switch (arguments.length) {
        case 1:
            callback = arguments[0];
            break;
        case 2:
            pool     = arguments[0];
            callback = arguments[1];
            break;
        case 3:
            pool     = arguments[0];
            opts     = arguments[1];
            callback = arguments[2];
            break;
        default:
            throw new Error('Invalid arguments');
    }
    opts.fields = opts.fields || zpool.listFields_;

    if (opts.fields.length != 1 && opts.fields[0] != 'name') {
        callback(notImplemented('zpool.list',
            'only supported with opts.fields = [ \'name\' ]'));
    }

    var pools = mockds.getPools();

    if (pool) {
        if (pools.indexOf(pool) === -1) {
            callback(zfsError(null,
                'cannot open \'' + pool + '\': no such pool'));
            return;
        }
        callback(null, opts.fields, [[pool]]);
        return;
    }

    callback(null, opts.fields, pools.map(function (x) { return [ x ]; }));
};

zpool.status = function (pool, callback) {
    if (arguments.length != 2)
        throw new Error('Invalid arguments');

    if (mockds.getPools().indexOf(pool) === -1) {
        callback(null, 'UNKNOWN');
        return;
    }
    callback(null, 'ONLINE');
};

/*
 * zpool.create()
 *
 * This allows fine-grained control and exposes all features of the
 * zpool create command, including log devices, cache devices, and hot spares.
 * The input is an object of the form produced by the disklayout library.
 */
zpool.create = function (pool, _config, callback) {
    var args;

    if (arguments.length != 3) {
        throw new Error('Invalid arguments, 3 arguments required');
    }

    try {
        new Dataset(null, pool, 'filesystem');
    } catch (err) {
        if (err.name === 'DatasetExistsError') {
            callback(zfsError(err,
                'cannot create \'' + pool + '\': pool already exists'));
        } else {
            callback(zfsError(err, 'unexpected failure'));
        }
        return
    }

    callback();
};

zpool.destroy = function (pool, callback) {
    if (arguments.length != 2)
        throw new Error('Invalid arguments');

    try {
        mockds.destroyPool(pool)
    } catch (err) {
        callback(zfsError(err, 'cannot open \'' + pool + '\': no such pool'));
        return;
    }
    callback();
}

zpool.upgrade = function (pool) {
    var version = -1,
        callback;
    if (arguments.length === 2) {
        callback = arguments[1];
    } else if (arguments.length === 3) {
        version = arguments[1];
        callback = arguments[2];
    } else {
        throw new Error('Invalid arguments');
    }

    callback(notImplemented('zpool.upgrade'));
};

var zfs;
exports.zfs = zfs = function () {};

function getParentName(name) {
    var hierarchy = name.split('/');
    if (hierarchy.length < 2) {
        throw new Error('\'' + name + '\' has no parent');
    }
    hierarchy.pop();
    return hierarchy.join('/');
}

zfs.create = function (name, callback) {
    if (arguments.length != 2)
        throw new Error('Invalid arguments');

    try {
        var ma = mockds.get(getParentName(name));
    } catch (err) {
        if (err.message.match(/has no parent/)) {
            err = zfsError(null, 'cannot create \'' + name +
                '\': missing dataset name');
        }
        callback(err);
        return
    }
    if (ma === null) {
        callback(zfsError(null,
            'cannot create \'' + name + '\': parent does not exist'));
        return;
    }

    try {
        new Dataset(ma, path.basename(name), 'filesystem');
    } catch (err) {
        if (err.name === 'DatasetExistsError') {
            err = zfsError(err,
                'cannot create \'' + name + '\': dataset already exists');
        }
        callback(err);
        return;
    }
    callback();
};

zfs.set = function (name, properties, callback) {
    if (arguments.length != 3)
        throw new Error('Invalid arguments');

    var ds = mockds.get(name);
    if (ds === null) {
        callback(zfsError(null,
            'cannot open \'' + name + '\': dataset does not exist'));
        return;
    }

    /*
     * Should be improved to:
     * - scale human numbers (e.g. 30g) to integers
     * - only allow valid properties
     * - do not allow setting of read-only properties
     * - fix up descendant mount points (really, this should be dynamic)
     */
    try {
        for (var prop in properties) {
            ds[prop] = properties[prop];
        }
    } catch (err) {
        callback(err);
        return;
    }
    callback();
};

zfs.get = function (name, propNames, parseable, callback) {
    if (arguments.length != 4)
        throw new Error('Invalid arguments');

    if (!parseable) {
        callback(notImplemented('zfs.get', 'without parseable=true'));
        return;
    }

    var values = [];

    function doProp(ds) {
        for (let prop in propNames) {
            prop = propNames[prop];
            values.push([ ds.name, prop, ds[prop] ]);
        }
    }

    try {
        if (name) {
            let ds = mockds.get(name);
            if (ds === null) {
                callback(zfsError(null,
                    'cannot open \'' + name + '\': dataset does not exist'));
                return;
            }
            doProp(ds);
            callback(null, values);
            return;
        }

        // Get for all pools

        let pools = mockds.getPools();
        for (let pool in pools) {
            root = mockds.get(dataset[pool]);

            for (let ds of root.iterDescendants(['all'])) {
                doProp(ds);
            }
        }

        callback(null, values);
    } catch (err) {
        callback(err);
    }
};

zfs.snapshot = function (name, callback) {
    if (arguments.length != 2) {
        throw new Error('Invalid arguments');
    }

    var fsname, snapname;
    [fsname, snapname] = name.split('@');
    if (!snapname) {
        callback(zfsError(null, 'cannot create snapshot \'' + name + '\': ' +
            'empty component or misplaced \'@\' or \'#\' delimiter in name'));
        return;
    }
    var ds = mockds.get(fsname);
    if (ds === null) {
        callback(zfsError(null,
            'cannot open \'' + name + '\': dataset does not exist'));
        return;
    }

    try {
        ds.snapshot(snapname);
    } catch (err) {
        if (err.name === 'DatasetExistsError') {
            err = zfsError(err, 'cannot create snapshot \'' + name + '\':' +
                'dataset already exists');
        }
        callback(err);
        return;
    }

    callback()
};

zfs.clone = function () {
    var snapshot, name, properties, callback;
    switch (arguments.length) {
        case 3:
            snapshot   = arguments[0];
            name       = arguments[1];
            properties = {};
            callback   = arguments[2];
            break;
        case 4:
            snapshot   = arguments[0];
            name       = arguments[1];
            properties = arguments[2];
            callback   = arguments[3];
            break;
        default:
            throw new Error('Invalid arguments');
    }
    assert.string(snapshot, 'snapshot');
    assert.string(name, 'name');
    assert.object(properties, 'properties');
    assert.func(callback, 'callback');

    if (name.indexOf('@') !== -1) {
        callback(zfsError(null, 'cannot create \'' + name +
            '\': snapshot delimiter \'@\' is not expected here'));
        return;
    }

    ssds = mockds.get(snapshot);
    if (ssds === null) {
        callback(zfsError(null,
            'cannot open \'' + snapshot + '\': dataset does not exist'));
        return;
    }
    try {
        ssds.clone(name, {}, properties);
    } catch (err) {
        if (err.name === 'DatasetExistsError') {
            err = zfsError(err, 'cannot create \'' + name + '\':' +
                'dataset already exists');
        }
        callback(err);
        return;
    }

    callback();
};

function destroy(name, recursive, callback) {
    assert.string(name, 'name');
    assert.bool(recursive, 'recursive');
    assert.func(callback, 'callback');
    if (arguments.length !== 3)
        throw new Error('Invalid arguments');

    let ds = mockds.get(name);
    if (ds === null) {
        callback(zfsError(null,
            'cannot open \'' + name + '\': dataset does not exist'));
        return;
    }

    try {
        ds.destroy({ recursive: recursive });
    } catch (err) {
        let msg;
        switch (err.name) {
            case 'SnapshotHoldError':
                err = new zfsError(err,
                    sprintf('cannot destroy \'%s\': dataset is busy', name));
                break;
            case 'DescendantError':
                msg = sprintf('cannot destroy \'%s\': %s has children', name,
                    ds.type);
                let kids = [];
                for (let kid of ds.iterDescendants([ 'all' ])) {
                    if (kid !== ds) {
                        kids.push(kid.name);
                    }
                }

                msg += '\nuse \'-r\' to destroy the following datasets:\n';
                msg += kids.reverse().join('\n');
                err = new zfsError(err, msg);
                break;
           case 'DependantError':
                msg = sprintf(
                    'cannot destroy \'%s\': snapshot has dependent clones',
                    name);
                msg += '\nuse \'-R\' to destroy the following datasets:\n';

                for (let kid of ds.iterDescendants(
                    [ 'filesystem', 'volume', 'clones' ])) {

                    msg += '\n' + kid.name;
                }
                err = new zfsError(err, msg);
                break;
        }
        callback(err);
        return;
    }

    callback();
};

zfs.destroy = function (name, callback) {
    if (arguments.length != 2)
        throw new Error('Invalid arguments');

    destroy(name, false, callback);
}

zfs.destroyAll = function (name, callback) {
    if (arguments.length != 2)
        throw new Error('Invalid arguments');

    destroy(name, true, callback);
};

/*
 * zfs.list fields
 */

zfs.listFields_ = [ 'name', 'used', 'avail', 'refer', 'type', 'mountpoint' ];

/*
 * List datasets.
 *
 * @param {String} [name]
 *   Dataset to list. If name is not given, `list` defaults to returning all
 *   datasets.
 *
 * @param {Object} [options]
 *   Options object:
 *     - `type`: restrict dataset type (dataset, volume, snapshot or all)
 *
 * @param {Function} [callback]
 *   Call `callback` when done. Function will be called with an error
 *   parameter, a field names list and a array of arrays comprising the list
 *   information.
 *
 */

zfs.list = function () {
    var datasets, callback;
    var options = {};

    switch (arguments.length) {
        case 1:
            datasets = mockds.getPools();
            options.recursive = true;
            callback = arguments[0];
            break;
        case 2:
            datasets = [ arguments[0] ];
            callback = arguments[1];
            break;
        case 3:
            datasets = [ arguments[0] ];
            options  = arguments[1];
            callback = arguments[2];
            break;
        default:
            throw new Error('Invalid arguments');
    }

    var types;
    if (options.type) {
        types = options.type.split(',');
    } else {
        types = [ 'filesystem', 'volume' ];
    }
    var recursive = options.recursive || false;
    var fields = options.fields || zfs.listFields_;
    var parseable = options.parseable || false;

    assert.bool(recursive, 'recursive');
    assert.arrayOfString(fields, 'fields');
    assert.bool(parseable, 'parseable');
    assert.func(callback, 'callback');

    if (!parseable) {
        callback(notImplemented('zfs.list', 'parseable=false not implemented'));
        return;
    }

    var rows = [];

    function do_one(ds) {
        let row = [];
        for (let prop of fields) {
            row.push(ds[prop]);
        }
        rows.push(row);
    }

    for (let ds in datasets) {
        ds = mockds.get(datasets[ds]);
        if (ds === null) {
            callback(zfsError(null,
                'cannot open \'' + dataset[ds] + '\': dataset does not exist'));
            return;
        }

        // Most properties will throw a 'not implemented' error.
        try {
            if (!recursive) {
                do_one(ds)
                continue;
            }

            for (ds of ds.iterDescendants(types)) {
                do_one(ds);
            }
        } catch (err) {
            callback(err);
            return;
        }
    }

    callback(null, fields, rows);
};

zfs.send = function (_snapshot, _filename, _callback) {
    throw notImplemented('zfs.send');
};

zfs.receive = function (_name, _filename, _callback) {
    throw notImplemented('zfs.receive');
};

zfs.list_snapshots = function () {
    var snapshot, callback;
    switch (arguments.length) {
        case 1:
            callback = arguments[0];
            break;
        case 2:
            snapshot = arguments[0];
            callback = arguments[1];
            break;
        default:
            throw new Error('Invalid arguments');
    }
    zfs.list(snapshot, { type: 'snapshot' }, callback);
};

zfs.rollback = function (_name, callback) {
    if (arguments.length != 2)
        throw new Error('Invalid arguments');

    throw notImplemented('zfs.rollback');

    callback();
};

zfs.rename = function (name, newname, callback) {
    if (arguments.length != 3)
        throw new Error('Invalid arguments');

    let ds = mockds.get(name);
    if (ds === null) {
        callback(zfsError(null,
            'cannot open \'' + name + '\': dataset does not exist'));
        return;
    }

    try {
        ds.rename(newname);
    } catch (err) {
        // XXX need to match `zfs rename`, as in commented code below
        callback(err);
        return;
    }

    /*
    if (name.indexOf('@') !== -1 || newname.indexOf('@') !== -1) {
        throw notImplemented('zfs.rename',
            'renaming of snapshots not supported');
    }

    if (newname.startsWith(name + '/')) {
        callback(zfsError(null, `cannot rename '${name}': ` +
            'New dataset name cannot be a descendant of current dataset name'));
        return;
    }

    var ds = getDs(name);
    if (oparent === null) {
        callback(zfsError(null,
            `cannot open '${name}': dataset does not exist`));
        return;
    }
    var oparent = ds.parent;

    var nparent = dsParent(newname);
    if (nparent === null) {
        callback(zfsError(null,
            `cannot create '${newname}': parent does not exist`));
        return;
    }

    var nleaf = path.basename(newname)
    if (nparent.children.hasOwnItem(nleaf)) {
        callback(zfsError(null,
            `cannot rename '${name}': dataset already exists`));
        return;
    }
    */

    callback();
};

zfs.upgrade = function (name, version, callback) {
    if (arguments.length === 2) {
        callback = arguments[1];
    } else if (arguments.length === 3) {
        version = arguments[1];
        callback = arguments[2];
    } else {
        throw new Error('Invalid arguments');
    }

    name = arguments[0];

    throw notImplemented('zfs.upgrade');
};

zfs.holds = function (snapshot, callback) {
    if (arguments.length != 2)
        throw new Error('Invalid arguments');

    var ds = mockds.get(snapshot);
    if (ds === null) {
        callback(zfsError(null,
            'cannot hold \'' + snapshot + '\': dataset does not exist'));
        return;
    }
    callback(null, Array.from(ds.holds()));
};

zfs.hold = function (snapshot, reason, callback) {
    if (arguments.length != 3)
        throw new Error('Invalid arguments');

    var ds = mockds.get(snapshot);
    if (ds === null) {
        callback(zfsError(null, 'cannot hold snapshot \'' + snapshot +
            '\': dataset does not exist'));
        return;
    }
    if (ds.holds().has(reason)) {
        callback(zfsError(null, 'cannot hold snapshot \'' + snapshot +
            '\': tag already exists on this dataset'));
        return;
    }
    ds.hold(reason);
    callback();
};

zfs.releaseHold = function (snapshot, reason, callback) {
    if (arguments.length != 3)
        throw new Error('Invalid arguments');

    var ds = mockds.get(snapshot);
    if (ds === null) {
        callback(zfsError(null,
            'cannot hold \'' + snapshot + '\': dataset does not exist'));
        return;
    }
    if (!ds.holds().has(reason)) {
        callback(zfsError(null, 'cannot release hold from snapshot \'' +
            snapshot + '\' no such tag on this dataset'));
        return;
    }
    ds.release(reason);
    callback();
};
