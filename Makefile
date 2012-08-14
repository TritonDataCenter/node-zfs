#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
#

#
# Tools
#
TAP		:= ./node_modules/.bin/tap
NPM		:= npm

#
# Files
#
DOC_FILES	 = index.restdown boilerplateapi.restdown
JS_FILES	:= $(shell find lib test -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
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
