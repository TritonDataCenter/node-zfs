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

const sprintf = require('sprintf-js').sprintf;
const { zfs, zpool } = require ('../lib/mock-zfs.js');
const mockds = require ('../lib/mock-dataset.js');

const Dataset = mockds.Dataset;

/**
 * Compares two nested lists of the form [ [ 'pool' ], [ 'pool2' ] ]
 */
function checkPoolList(t, have, want) {
    var s1 = new Set(have.map(function (x) {
        t.equal(x.length, 1, sprintf('"%j" has one element', x));
        return x[0];
    }));
    var s2 = new Set(want.map(function (x) {
        t.equal(x.length, 1, sprintf('"%j" has one element', x));
        return x[0];
    }));
    var val;

    t.equal(s1.size, have.length, 'have array has unique elements');
    t.equal(s2.size, want.length, 'want array has unique elements');

    for (val of s2) {
        t.ok(s1.delete(val), sprintf('%j was found in have array', val));
    }
    for (val of s1) {
        t.equal(val, null, 'no unexpected values in have array');
    }
}

tap.test('zpool', (tt) => {
    tt.afterEach(function (done) {
        mockds.reset();
        done();
    });

    tt.test('zpool', (t) => {
        // mock zpool ignores the config (second arg).
        zpool.create('pool1', {}, function (err) {
            t.error(err, 'create pool1');
        });

        zpool.create('pool1', {}, function (err) {
            t.notEqual(err || null, null,
                'zpool create pool1 should fail when it already exists');
            var msg = err ? err.message : '<no error>';
            t.match(msg, /pool already exists/, '"pool already exists" error');
        });

        // mock zpool only supports name.
        zpool.list('pool1', { fields: [ 'name' ] },
            function (err, fields, values) {

            t.error(err, 'zpool list pool1 does not return an error');
            checkPoolList(t, values, [ [ 'pool1' ] ]);
        });

        zpool.status('pool1', function (err, val) {
            t.error(err, 'zpool status pool1 does not have an error');
            t.equal(val, 'ONLINE', 'pool1 is online');
        });

        zpool.status('pool2', function (err, val) {
            t.error(err, 'zpool status pool2 does not have an error');
            t.equal(val, 'UNKNOWN', 'pool2 is unknown');
        });

        zpool.create('pool2', {}, function (err) {
            t.error(err, 'create pool2');
        });

        zpool.status('pool2', function (err, val) {
            t.error(err, 'zpool status pool2 does not have an error');
            t.equal(val, 'ONLINE', 'pool2 is online after creation');
        });

        zpool.list('pool2', { fields: [ 'name' ] },
            function (err, fields, values) {

            t.error(err, 'zpool list pool2 does not return an error');
            checkPoolList(t, values, [ [ 'pool2' ] ]);
        });

        zpool.list(null, { fields: [ 'name' ] },
            function (err, fields, values) {

            t.error(err, 'zpool list does not return an error');
            checkPoolList(t, values, [ [ 'pool1' ], [ 'pool2' ] ]);
        });

        zpool.destroy('pool1', function (err) {
            t.error(err, 'zpool destroy pool1 does not return an error');
        });

        zpool.destroy('pool1', function (err) {
            t.notEqual(err || null, null,
                'zpool destroy pool1 should fail the second time');
            var msg = err ? err.message : '<no error>';
            t.match(msg, /no such pool/, '"no such pool" error');
        });

        zpool.list(null, { fields: [ 'name' ] },
            function (err, fields, values) {

            t.error(err, 'zpool list does not return an error');
            checkPoolList(t, values, [ [ 'pool2' ] ]);
        });

        zpool.list('pool2', { fields: [ 'name' ] },
            function (err, fields, values) {

            t.error(err, 'zpool list pool2 does not return an error');
            checkPoolList(t, values, [ [ 'pool2' ] ]);
        });

        t.end();
    });

    tt.test('zfs', (t) => {
        zfs.create('nosuchpool', function (err) {
            t.notEqual(err || null, null, 'cannot create a top-level dataset');
            var msg = err ? err.message : '<no error>';
            t.match(msg, /missing dataset name/,
                '"missing dataset name" error');
        });

        zfs.create('nosuchpool/foo', function (err) {
            t.notEqual(err || null, null,
                'cannot create a dataset in a non-existent pool');
            var msg = err ? err.message : '<no error>';
            t.match(msg, /parent does not exist/,
                '"parent does not exist" error');
        });

        zpool.create('testpool', {}, function (err) {
            t.error(err, 'zfs create testpool');
        });

        zfs.create('testpool/foo', function (err) {
            t.error(err, 'zfs create testpool/foo');
        });

        zfs.create('testpool/foo', function (err) {
            t.notEqual(err || null, null,
                'zfs create testpool/foo fails if dataset exists');
            var msg = err ? err.message : '<no error>';
            t.match(msg, /dataset already exists/,
                '"dataset already exists" error');
        });

        zfs.create('testpool/foo/bar', function (err) {
            t.error(err, 'zfs create testpool/foo/bar');
        });

        zfs.rename('testpool/foo/bar', 'testpool/baz', function (err) {
            t.error(err, 'zfs rename testpool/foo/bar testpool/baz');
        });

        zfs.get('testpool/baz', [ 'name', 'mountpoint' ], true,
            function (err, values) {

            t.error(err, 'zfs get name testpool/baz');
            t.equal(values.length, 2, 'two values returned');
            t.equal(values[0][0], 'testpool/baz', 'row 1 col 1 has dsname');
            t.equal(values[0][1], 'name', 'row 1 col 2 has propname');
            t.equal(values[0][2], 'testpool/baz', 'row 1 col 3 has value');
            t.equal(values[1][0], 'testpool/baz', 'row 2 col 1 has dsname');
            t.equal(values[1][1], 'mountpoint', 'row 2 col 2 has propname');
            t.equal(values[1][2], '/testpool/baz', 'row 2 col 3 has value');
        });

        zfs.set('testpool/baz', { mountpoint: '/baz' }, function (err) {
            t.error(err, 'zfs set mountpoint=/baz testpool/baz');
        });

        zfs.get('testpool/baz', [ 'mountpoint' ], true,
            function (err, values) {

            t.error(err, 'zfs get name testpool/baz');
            t.equal(values.length, 1, 'one value returned');
            t.equal(values[0][0], 'testpool/baz', 'row 1 col 1 has dsname');
            t.equal(values[0][1], 'mountpoint', 'row 1 col 2 has propname');
            t.equal(values[0][2], '/baz', 'row 1 col 3 has value');
        });

        zfs.rename('testpool/baz', 'testpool/foo/blah', function (err) {
            t.error(err, 'zfs rename testpool/baz testpool/foo/blah');
        });

        zfs.destroy('testpool/foo/blah', function (err) {
            t.error(err, 'zfs destroy testpool/foo/blah');
        });

        zfs.snapshot('testpool/foo@snap1', function (err) {
            t.error(err, 'zfs snapshot testpool/foo@snap1');
        });

        zfs.list('testpool', {
            fields: [ 'name' ],
            parseable: true
        }, function (err, fields, values) {
            t.error(err, 'zfs list -o name testpool');
            t.equals(fields.length, 1, 'one field');
            t.equals(fields[0], 'name', 'the one field is "name"');
            checkPoolList(t, values, [ [ 'testpool' ] ]);
        });

        zfs.list('testpool', {
            fields: [ 'name' ],
            parseable: true,
            recursive: true
        }, function (err, fields, values) {
            t.error(err, 'zfs list -o name testpool');
            t.equals(fields.length, 1, 'one field');
            t.equals(fields[0], 'name', 'the one field is "name"');
            checkPoolList(t, values, [
                [ 'testpool' ],
                [ 'testpool/foo' ]
            ]);
        });

        zfs.list('testpool', {
            fields: [ 'name' ],
            parseable: true,
            recursive: true,
            type: 'snapshot'
        }, function (err, fields, values) {
            t.error(err, 'zfs list -o name -t snapshot testpool');
            t.equals(fields.length, 1, 'one field');
            t.equals(fields[0], 'name', 'the one field is "name"');
            checkPoolList(t, values, [
                [ 'testpool/foo@snap1' ]
            ]);
        });

        zfs.list('testpool', {
            fields: [ 'name' ],
            parseable: true,
            recursive: true,
            type: 'all'
        }, function (err, fields, values) {
            t.error(err, 'zfs list -o name -t all testpool');
            t.equals(fields.length, 1, 'one field');
            t.equals(fields[0], 'name', 'the one field is "name"');
            checkPoolList(t, values, [
                [ 'testpool' ],
                [ 'testpool/foo' ],
                [ 'testpool/foo@snap1' ]
            ]);
        });

        zfs.list_snapshots('testpool/foo@snap1', function (err, fields, values) {
            t.notEqual(err || null, null,
                '"zfs list_snapshots (2 args) not fully implemented');
            t.match(err ? err.message : '<no error>', /not implemented/,
                'error message contains "not implemented"');
        });

        zfs.list_snapshots(function (err, fields, values) {
            t.notEqual(err || null, null,
                '"zfs list_snapshots (1 arg) not fully implemented');
            t.match(err ? err.message : '<no error>', /not implemented/,
                'error message contains "not implemented"');
        });

        zfs.clone('testpool/foo@snap1', 'testpool/bar', function (err) {
            t.error(err, 'zfs clone testpool/foo@snap1 testpool/bar');
        });

        zfs.snapshot('testpool/bar@snap2', function (err) {
            t.error(err, 'zfs snapshot testpool/bar@snap2');
        });

        zfs.hold('testpool/bar@snap2', 'something', function (err) {
            t.error(err, 'zfs hold something testpool/bar@snap2');
        });

        zfs.destroy('testpool/bar@snap2', function (err) {
            t.notEqual(err || null, null,
                '"zfs destroy testpool/bar@snap2" should fail due to hold');
            t.match(err ? err.message : '<no error>', /dataset is busy/,
                'error message contains "dataset is busy"');
        });

        zfs.destroyAll('testpool/bar@snap2', function (err) {
            t.notEqual(err || null, null,
                '"zfs destroy -r testpool/bar@snap2" should fail due to hold');
            t.match(err ? err.message : '<no error>', /dataset is busy/,
                'error message contains "dataset is busy"');
        });

        zfs.releaseHold('testpool/bar@snap2', 'something', function (err) {
            t.error(err, 'zfs release something testpool/bar@snap2');
        });

        zfs.destroy('testpool/bar@snap2', function (err) {
            t.error(err, 'zfs destroy testpool/bar@snap2');
        });

        zfs.destroyAll('testpool/foo', function (err) {
            t.notEqual(err || null, null,
                '"zfs destroy -r testpool/foo" should fail due to clone');
            t.match(err ? err.message : '<no error>', /has dependent clones/,
                'error message contains "has dependent clones"');
        });

        zfs.destroy('testpool/bar', function (err) {
            t.error(err, 'zfs destroy testpool/bar');
        });

        zfs.destroy('testpool/foo', function (err) {
            t.notEqual(err || null, null,
                '"zfs destroy testpool/foo" should fail due to snapshot');
            t.match(err ? err.message : '<no error>', /has children/,
                'error message contains "has children"');
        });

        zfs.destroyAll('testpool/foo', function (err) {
            t.error(err, 'zfs destroy -r testpool/foo');
        });

        t.end();
    });

    tt.end();
});

