var sys   = require('sys')
  , spawn = require('child_process').spawn;

var puts = sys.puts;

var ZPOOL_PATH = '/usr/sbin/zpool'
  , ZFS_PATH   = '/usr/sbin/zfs';

exports.zpool = zpool = function () { }

zpool.listFields_ = [ 'name', 'size', 'used', 'available'
                    , 'capacity', 'health', 'altroot' ];

zpool.list = function (callback) {
  var proc = spawn(ZPOOL_PATH, ['list', '-H']);
  var buffer = '';

  proc.stdout.addListener('data', function (data) {
    buffer = buffer + data;
  });

  proc.addListener('exit', function (code) {
    buffer = buffer.replace(/(^\s+|\s+$)/, '');
    lines = parseTabSeperatedTable(buffer);
    callback({ fields: zpool.listFields_, zpools: lines });
  });
};

function parseTabSeperatedTable(data) {
  var lines = data.split("\n");
 
  var i, l;
  for (i=0, l=lines.length; i < l; i++) {
    lines[i] = lines[i].split("\t");
  }
 
  return lines;
}

exports.zfs = zfs = function () {}

zfs.listFields_ = [ 'name', 'used', 'available', 'referenced', 'mountpoint' ];

zfs.list = function (callback) {
  var proc = spawn(ZFS_PATH, ['list', '-H']);
  var buffer = '';

  proc.stdout.addListener('data', function (data) {
    buffer = buffer + data;
  });

  proc.addListener('exit', function (code) {
    buffer = buffer.replace(/(^\s+|\s+$)/, '');
    lines = parseTabSeperatedTable(buffer);
    callback({ fields: zfs.listFields_, zfs: lines });
  });
};
