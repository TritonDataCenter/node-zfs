var sys      = require('sys')
  , cp       = require('child_process');

var execFile = cp.execFile
  , spawn    = cp.spawn
  , puts     = sys.puts
  , inspect  = sys.inspect;

var ZPOOL_PATH = '/usr/sbin/zpool'
  , ZFS_PATH   = '/usr/sbin/zfs';

exports.zpool = zpool = function () { }

zpool.listFields_ =
    [ 'name', 'size', 'used', 'avail' , 'cap', 'health', 'altroot' ];

// if zfs commands take longer than timeoutDuration it's an error
timeoutDuration = exports.timeoutDuration = 5000;

zpool.list = function () {
  var pool, callback;
  switch (arguments.length) {
    case 1:
      callback = arguments[0];
      break;
    case 2:
      pool     = arguments[0];
      callback = arguments[1];
      break;
    default:
      throw Error('Invalid arguments');
  }
  var args = ['list', '-H'];
  if (pool) args.push(pool);

  execFile(ZPOOL_PATH, args, { timeout: timeoutDuration },
    function (err, stdout, stderr) {
      stdout = stdout.trim();
      if (err) {
        err.msg = stderr;
        callback(err);
        return;
      }
      if (stdout == "no pools available\n") {
        callback(err, zfs.listFields_, []);
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

zfs.set = function (name, properties, callback) {
  if (arguments.length != 3) {
    throw Error('Invalid arguments');
  }

  var keys = Object.keys(properties);

  // loop over and set all the properties using chained callbacks
  (function () {
    var next = arguments.callee;
    if (!keys.length) {
      callback();
      return;
    }
    var key = keys.pop();

    execFile(ZFS_PATH, ['set', key + '=' + properties[key], name],
      { timeout: timeoutDuration },
      function (err, stdout, stderr) {
        next(); // loop by calling enclosing function
      });
  })();
}

zfsGetRegex = new RegExp("^([^\t]+)\t([^\t]+)\t(.+)");
zfs.get = function (name, propNames, callback) {
  if (arguments.length != 3) {
    throw Error("Invalid arguments");
  }

  execFile(ZFS_PATH,
    ['get', '-H', '-o', 'source,property,value', propNames.join(','), name],
    { timeout: timeoutDuration },
    function (err, stdout, stderr) {
      var properties = {};

      // Populate the properties hash with regexp match groups from each line.
      // Break on  first empty line
      var lines = stdout.split("\n");
      var i,l,m;
      for (i=0,l=lines.length;i<l;i++) {
        var m = zfsGetRegex.exec(lines[i]);
        if (!m) continue;
        properties[m[2]] = m[3];
      }
      callback(null, properties);
    });
}

zfs.snapshot = function (name, callback) {
  if (arguments.length != 2) {
    throw Error('Invalid arguments');
  }
  execFile(ZFS_PATH, ['snapshot', name], { timeout: timeoutDuration }, callback);
}

zfs.clone = function (snapshot, name, callback) {
  if (arguments.length != 3) {
    throw Error('Invalid arguments');
  }
  execFile(ZFS_PATH, ['clone', snapshot, name],
           { timeout: timeoutDuration }, callback);
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

zfs.listFields_ = [ 'name', 'used', 'avail', 'refer', 'mountpoint' ];

// zfs.list(callback) - list all datasets
// zfs.list(dataset, callback) -  list specific dataset
zfs.list = function () {
  var dataset, callback;
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
  var args = ['list', '-H', '-t', 'filesystem'];
  if (dataset) args.push(dataset);

  execFile(ZFS_PATH, args,
    { timeout: timeoutDuration },
    function (err, stdout, stderr) {
      stdout = stdout.trim();
      if (err) {
        err.msg = stderr;
        return callback(err);
      }
      if (stdout == "no datasets available\n") {
        callback(err, zfs.listFields_, []);
        return;
      }
      lines = parseTabSeperatedTable(stdout);
      callback(err, zfs.listFields_, lines);
    });
};

zfs.send = function (snapshot, filename, callback) {
  fs.open(filename, 'w', 400, function (err, fd) {
    if (err) {
      callback(err);
      return;
    }
    // set the child to write to STDOUT with `fd`
    var child = spawn(ZFS_PATH, ['send', snapshot], undefined, [-1, fd]);
    child.addListener('exit', function (code) {
      if (code) {
        callback("Return code was " + code);
        return;
      }
      fs.close(fd, function () {
        callback(null);
      });
    });
  });
}

zfs.receive = function (name, filename, callback) {
  fs.open(filename, 'r', 400, function (err, fd) {
    if (err) {
      callback(err);
      return;
    }
    // set the child to read from STDIN with `fd`
    var child = spawn(ZFS_PATH, ['receive', name], undefined, [fd]);
    child.addListener('exit', function (code) {
      if (code) {
        callback("Return code was " + code);
        return;
      }
      fs.close(fd, function () {
        callback(null);
      });
    });
  });
}

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
      throw Error('Invalid arguments');
  }
  var args = ['list', '-H', '-t', 'snapshot'];
  if (snapshot) args.push(snapshot);

  execFile(ZFS_PATH, args,
    { timeout: timeoutDuration },
    function (err, stdout, stderr) {
      stdout = stdout.trim();
      if (err) {
        err.msg = stderr;
        return callback(err);
      }
      if (stdout == "no datasets available\n") {
        callback(err, zfs.listFields_, []);
        return;
      }
      lines = parseTabSeperatedTable(stdout);
      callback(err, zfs.listFields_, lines);
    });
};

zfs.rollback = function (name, callback) {
  if (arguments.length != 2) {
    throw Error('Invalid arguments');
  }
  execFile(ZFS_PATH, ['rollback', name], { timeout: timeoutDuration }, callback);
}
