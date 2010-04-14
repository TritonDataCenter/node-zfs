var sys      = require('sys')
  , execFile = require('child_process').execFile;

var puts = sys.puts;

var ZPOOL_PATH = '/usr/sbin/zpool'
  , ZFS_PATH   = '/usr/sbin/zfs';

exports.zpool = zpool = function () { }

zpool.listFields_ =
    [ 'name', 'size', 'used', 'available' , 'capacity', 'health', 'altroot' ];

// if zfs commands take longer than timeoutDuration it's an error
timeoutDuration = exports.timeoutDuration = 5000;

zpool.list = function () {
  var dataset='', callback;
  switch (arguments.length) {
    case 1:
      callback = arguments[0];
      break;
    case 2:
      dataset = arguments[0];
      callback = arguments[1];
      break;
    default:
      throw Error('Invalid arguments');
  }

  execFile(ZPOOL_PATH, ['list', '-H', 'dataset'], { timeout: timeoutDuration },
    function (err, stdout, stderr) {
    stdout = stdout.trim();
    if (err) {
      err.msg = stderr;
      callback(err);
      return;
    }
    lines = parseTabSeperatedTable(stdout);
    callback(err, zpool.listFields_, lines);
  });
};

function parseTabSeperatedTable(data) {
  var i, l, lines = data.split("\n");
  for (i=0, l=lines.length; i < l; i++) {
    lines[i] = lines[i].split("\t");
  }
  return lines;
}

exports.zfs = zfs = function () {}

zfs.create = function (name, callback) {
  if (arguments.length != 2) {
    throw Error('Invalid arguments');
  }
  execFile(ZFS_PATH, ['create', name], { timeout: timeoutDuration }, callback);
}

zfs.snapshot = function (name, callback) {
  if (arguments.length != 2) {
    throw Error('Invalid arguments');
  }
  execFile(ZFS_PATH, ['snapshot', name], { timeout: timeoutDuration }, callback);
}

zfs.destroy = function (name, callback) {
  if (arguments.length != 2) {
    throw Error('Invalid arguments');
  }
  execFile(ZFS_PATH, ['destroy', name], { timeout: timeoutDuration }, callback);
}

zfs.destroyAll = function (name, callback) {
  if (arguments.length != 2) {
    throw Error('Invalid arguments');
  }
  execFile(ZFS_PATH, ['destroy', '-r',  name], { timeout: timeoutDuration }, callback);
}

zfs.listFields_ = [ 'name', 'used', 'available', 'referenced', 'mountpoint' ];

// zfs.list(callback) - list all datasets
// zfs.list(dataset, callback) -  list specific dataset
zfs.list = function () {
  var dataset='', options={}, callback;
  switch (arguments.length) {
    case 1:
      callback = arguments[0];
      break;
    case 2:
      dataset = arguments[0];
      callback = arguments[1];
      break;
    default:
      throw Error('Invalid arguments');
  }

  execFile(ZFS_PATH, ['list', '-t', 'all', '-H', dataset],
    { timeout: timeoutDuration },
    function (err, stdout, stderr) {
      stdout = stdout.trim();
      if (err) {
        err.msg = stderr;
        return callback(err);
      }
      lines = parseTabSeperatedTable(stdout);
      callback(err, zfs.listFields_, lines);
    });
};
