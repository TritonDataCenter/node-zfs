#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

#
# Tools
#
TAP		:= ./node_modules/.bin/tap
NPM		:= npm

#
# Files
#
JS_FILES	:= $(shell find lib test -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
# The mock files were written with es6 features, which jsl doesn't understand.
JSL_FILES_NODE  := $(shell find lib test -name '*.js' | grep -v mock)
JSSTYLE_FILES	 = $(JS_FILES)

include ./tools/mk/Makefile.defs

.PHONY: all
all: $(TAP)
	$(NPM) rebuild

$(TAP):
	$(NPM) install

CLEAN_FILES += $(TAP) ./node_modules/tap

.PHONY: test
test: $(TAP)
	TAP=1 $(TAP) test/*.test.js

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.targ
