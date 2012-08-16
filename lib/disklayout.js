/*
 * Copyright 2012 Joyent, Inc.  All rights reserved.
 */

require('/usr/node/node_modules/platform_node_version').assert();

/*
 * Returns the rounded-off capacity in GB.  The purpose of this is to
 * group devices of the same basic size class; disks and to a lesser extent
 * SSDs are marketed at a very limited number of capacity points although
 * the actual capacities vary quite a bit at each one.  We don't want to
 * get confused by that.
 */
function
round_capacity(bytes)
{
	var mb = bytes / 1000000;	/* thieves, that's what they are */
	var THRESHOLDS = [ 500000, 150000, 80000, 20000, 4500, 1000 ];
	var i;
	var bestfit, bestdiff = 0;

	for (i = 0; i < THRESHOLDS.length; i++) {
		var t = THRESHOLDS[i];
		var roundoff = Math.floor((mb + Math.floor(t / 2)) / t) * t;
		var multiplier = Math.pow(roundoff / t, 4);
		var diff = (mb - roundoff) * (mb - roundoff) * multiplier;

		if (Math.abs(mb - roundoff) / mb > 0.05)
			continue;

		if (diff < bestdiff || bestdiff === 0) {
			bestfit = roundoff;
			bestdiff = (mb - roundoff) * (mb - roundoff) *
			    multiplier;
		}
	}

	if (Math.abs(bestfit - mb) / mb < 0.05)
		return (bestfit / 1000);

	/*
	 * This device's size is not within +/-5% of any number of GB.
	 * That's very unusual and suggests we probably oughtn't use it.
	 * Round off to the nearest 1 GB and call it a day for now.  Most
	 * such devices are probably very small and we will return 0 for them.
	 */
	return (Math.round(mb / 1000));
}

function
merge_types(inv)
{
	Object.keys(inv).forEach(function (t0type) {
		Object.keys(inv).forEach(function (t1type) {
			var t0 = inv[t0type];
			var t1 = inv[t1type];

			if (t0type == t1type || !t0 || !t1)
				return;
			if (t0.solid_state != t1.solid_state)
				return;
			if (Math.abs(t0.size - t1.size) / t1.size > 0.05)
				return;

			if (t0.disks.length > t1.disks.length) {
				t0.disks = t0.disks.concat(t1.disks);
				delete inv[t1type];
			} else {
				t1.disks = t1.disks.concat(t0.disks);
				delete inv[t0type];
			}
		});
	});
}

function
shrink(inv)
{
	Object.keys(inv).forEach(function (typedesc) {
		var smallest = 0;
		var largest = 0;
		var type = inv[typedesc];

		type.disks.forEach(function (disk) {
			if (smallest === 0 || disk.size < smallest)
				smallest = disk.size;
			if (largest === 0 || disk.size > largest)
				largest = disk.size;
		});
		type.smallest = smallest;
		type.largest = largest;
		delete type.size;
	});
}

function
xform_bucket(bucket)
{
	var role = [];

	bucket.disks.forEach(function (disk) {
		role.push({
			name: disk.name,
			vid: disk.vid,
			pid: disk.pid,
			size: disk.size
		});
	});

	return (role.sort(function (a, b) {
		return (a.size - b.size);
	}));
}

/*
 * At this point we have detected the likely size bucket for each device, then
 * merged any buckets that required it.  We should have one bucket for each
 * approximate size, segregated by whether the devices are solid-state.  We
 * also know the smallest size of any device in each sub-bucket.  It's time to
 * assign each bucket a role.
 *
 * There are three possible roles: storage, cache, and slog.  If there is
 * only one bucket left, that's easy: it's storage.  Otherwise we're going
 * to make some judgment calls.  All spinning disks are for storage, always.
 * If there are 4 or fewer of the smallest SSD type, they're slogs.  Anything
 * else is a cache device, unless there were no spinning disks at all in
 * which case the largest devices will be used as primary storage.
 */
function
assign_roles(inv)
{
	var typedescs = Object.keys(inv);
	var ssddescs = [];
	var roles = {};

	if (typedescs.length === 0)
		return (roles);

	if (typedescs.length === 1) {
		roles.storage = xform_bucket(inv[typedescs[0]]);
		return (roles);
	}

	typedescs.forEach(function (typedesc) {
		var role;

		if (inv[typedesc].solid_state) {
			ssddescs.push(typedesc);
		} else {
			role = xform_bucket(inv[typedesc]);
			if (roles.storage)
				roles.storage = roles.storage.concat(role);
			else
				roles.storage = role;
		}
	});

	if (ssddescs.length === 0)
		return (roles);

	ssddescs.sort(function (a, b) {
		if (inv[a].smallest < inv[b].smallest)
			return (-1);
		if (inv[a].smallest > inv[b].smallest)
			return (1);
		return (0);
	});

	if (inv[ssddescs[0]].disks.length < 5) {
		roles.slog = xform_bucket(inv[ssddescs[0]]);
		ssddescs.splice(0, 1);
	}

	if (!roles.storage) {
		var largest = ssddescs.splice(ssddescs.length - 1, 1);
		roles.storage = xform_bucket(inv[largest]);
	}

	ssddescs.forEach(function (typedesc) {
		var role = xform_bucket(inv[typedesc]);
		if (roles.cache)
			roles.cache = roles.cache.concat(role);
		else
			roles.cache = role;
	});

	return (roles);
}

function
do_single(disks)
{
	var config = { vdevs: [] };
	config.vdevs[0] = disks[0];
	config.capacity = disks[0].size;

	return (config);
}

function
do_mirror(disks)
{
	var spares;
	var config = {};
	var capacity;

	if (disks.length < 2) {
		config.error = 'at least 2 disks are required for mirroring';
		return (config);
	}

	if (disks.length === 2) {
		spares = 0;
	} else {
		spares = Math.ceil(disks.length / 16);
		spares += (disks.length - spares) % 2;
	}

	/*
	 * The largest devices can spare for any others.  Not so for the
	 * smaller ones.
	 */
	if (spares > 0)
		config.spares = disks.splice(disks.length - spares, spares);

	config.vdevs = [];
	capacity = 0;
	while (disks.length) {
		var vdev = {};

		capacity += disks[0].size * 1;
		vdev.type = 'mirror';
		vdev.devices = disks.splice(0, 2);
		config.vdevs.push(vdev);
	}

	config.capacity = capacity;

	return (config);
}

function
do_raidz2(disks)
{
	var MINWIDTH = 7;
	var MAXWIDTH = 12;
	var config = {};
	var spares;
	var width;
	var capacity;

	spares = Math.min(2, Math.floor(disks.length / 12));

	while (disks.length - spares >= MINWIDTH) {
		for (width = MINWIDTH; width <= MAXWIDTH; width++) {
			if ((disks.length - spares) % width === 0)
				break;
		}
		if (width <= MAXWIDTH)
			break;
		++spares;
	}

	if (disks.length - spares < MINWIDTH) {
		config.error = 'no acceptable raidz2 layout is possible with ' +
		    disks.length + ' disks';
		return (config);
	}

	/*
	 * The largest devices can spare for any others.  Not so for the
	 * smaller ones.
	 */
	if (spares > 0)
		config.spares = disks.splice(disks.length - spares, spares);

	config.vdevs = [];
	capacity = 0;
	while (disks.length) {
		var vdev = {};

		capacity += (width - 2) * disks[0].size;
		vdev.type = 'raidz2';
		vdev.devices = disks.splice(0, width);
		config.vdevs.push(vdev);
	}

	config.capacity = capacity;

	return (config);
}

var LAYOUTS = {
	single: do_single,
	mirror: do_mirror,
	raidz2: do_raidz2
};

function
register_layout(name, f)
{
	if (typeof (name) !== 'string' || typeof (f) !== 'function')
		throw new TypeError('string and function arguments required');
	LAYOUTS[name] = f;
}

function
list_supported()
{
	return (Object.keys(LAYOUTS));
}

function
compute_layout(disks, layout)
{
	var disktypes = {};
	var diskroles;
	var config = {};

	config.input = disks;
	config.layout = layout;

	disks.forEach(function (disk) {
		var gb;
		var typespec;

		if (disk.removable)
			return;

		if ((gb = round_capacity(disk.size)) === 0)
			return;

		typespec = disk.type + ',' + gb + ',' + disk.solid_state;
		disk.rounded_size = gb;
		if (!disktypes[typespec]) {
			disktypes[typespec] = {
				type: disk.type,
				size: gb,
				solid_state: disk.solid_state,
				disks: []
			};
		}
		disktypes[typespec].disks.push(disk);
	});

	merge_types(disktypes);
	shrink(disktypes);
	diskroles = assign_roles(disktypes);

	if (!diskroles.storage) {
		config.error = 'no primary storage disks available';
		return (config);
	}

	if (!layout) {
		if (diskroles.storage.length == 1)
			layout = 'single';
		else if (diskroles.storage.length > 16)
			layout = 'raidz2';
		else
			layout = 'mirror';
	}

	if (!LAYOUTS[layout]) {
		config.error = 'unknown layout ' + layout;
		return (config);
	}

	config = LAYOUTS[layout](diskroles.storage);

	if (diskroles.slog)
		config.logs = diskroles.slog;
	if (diskroles.cache)
		config.cache = diskroles.cache;

	return (config);
}

module.exports = {
	register: register_layout,
	list_supported: list_supported,
	compute: compute_layout
};
