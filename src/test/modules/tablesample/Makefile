# src/test/modules/tsm_test/Makefile

MODULE_big = tsm_test
OBJS = tsm_test.o $(WIN32RES)
PGFILEDESC = "tsm_test - example of a custom tablesample method"

EXTENSION = tsm_test
DATA = tsm_test--1.0.sql

REGRESS = tablesample

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/tablesample
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
