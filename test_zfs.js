var sys = require('sys')
  , zfs = require('./zfs');

var puts = sys.puts;
var inspect = sys.inspect;

zfs.zpool.list(function (out) {
  puts(inspect(out));
});

zfs.zfs.list(function (out) {
  puts(inspect(out));
});
