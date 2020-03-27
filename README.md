<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2020 Joyent, Inc.
-->

# node-zfs

This repository is part of the Joyent SmartDataCenter project (SDC).  For
contribution guidelines, issues, and general documentation, visit the main
[SDC](http://github.com/joyent/sdc) project page.

node-zfs is a Node.js interface to ZFS tools

# SYNOPSIS

    // list datasets
    zfs.list(function (err, fields, data) {
      // ...
    });

    // list snapshots
    zfs.list_snapshots(function (err, fields, data) {
      // ...
    });

    // create a dataset
    zfs.create('mydataset', function (err) {
      // ...
    });

    // destroy a dataset or snapshot
    zfs.destroy('mydataset', function (err) {
      // ...
    });

    // recursively destroy a dataset
    zfs.destroyAll('mydataset', function (err) {
      // ...
    });

    // create a snapshot
    zfs.snapshot('mydataset@backup', function (err) {
      // ...
    });

    // rollback a snapshot
    zfs.rollback('mydataset@backup', function (err) {
      // ...
    });

    // clone a dataset
    zfs.clone('mydataset@backup', 'mynewdataset', function (err) {
      // ...
    });

    // set dataset properties
    zfs.set('mydataset', { 'test:key1': 'value'
                         , 'test:key2': 'value' }, function (err) {
      // ...
    });

    // get dataset properties
    zfs.get('mydataset', [ 'test:key1', 'test:key2' ],
      function (err, properties) {
        // ...
      });

    // rename a dataset (filesystem, volume, or snapshot)
    zfs.rename('pool/ds1', 'pool/newname', function (err) {
      // ...
    });

    // upgrade a filesystem to a new version
    zfs.upgrade('mydataset', 5, function (err) {
      // ...
    });

    // send a snapshot
    zfs.send('mydataset@backup', '/some/file', function (err) {
      // ...
    });

    // receive a snapshot
    zfs.receive('mydataset@backup', '/some/file', function (err) {
      // ...
    });

    // hold a snapshot (prevent from deletion)
    zfs.hold('mydataset@backup', 'some_tag', function (err) {
      // ...
    });

    // release hold from a snapshot
    zfs.releaseHold('mydataset@backup', 'some_tag', function (err) {
      // ...
    });

    // list the holds on a snapshot
    zfs.releaseHold('mydataset@backup', 'some_tag', function (err, holds) {
      // ...
    });


# DESCRIPTION

The node-zfs library provies a thin, evented wrapper around common ZFS
commands.  It also contains functionality to automatically generate pool
layouts based on a disk inventory.


# ENVIRONMENT

The library was developed on an OpenSolaris snv_111b system and has
subsequently been used on SmartOS and Linux.

# TESTING

The [mock-zfs.js](lib/mock-zfs.js) file allows consumers of this module to mock
it.  When used in conjection with
[mock-fs](https://www.npmjs.com/package/mock-fs), ZFS filesystem operations
(mount, unmount, clone, etc.) that would affect the visible filesystem
namespace will be represented in the mocked directory hierarchy.  Other
fs mockers may be used, so long as they cause `fs.stat().dev` to have the value
8675309.

# AUTHOR

Orlando Vazquez <orlando@joyent.com>
Keith Wesolowski <keith.wesolowski@joyent.com>


# SEE ALSO

zfs(1M), zpool(1M)
