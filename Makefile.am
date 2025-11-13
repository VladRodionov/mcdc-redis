# Top-level Automake
AM_CPPFLAGS = -I$(srcdir)/include -I$(srcdir)/deps/redis $(ZSTD_CFLAGS)

lib_LTLIBRARIES = mcdc.la
mcdc_la_SOURCES = \
  src/module.c \
  src/mcdc_dict.c src/mcdc_dict.h \
  src/mcdc_namespace.c src/mcdc_namespace.h \
  src/mcdc_rdb.c src/mcdc_rdb.h \
  src/mcdc_replication.c src/mcdc_replication.h \
  src/mcdc_util.c src/mcdc_util.h

mcdc_la_LDFLAGS = -module -avoid-version
mcdc_la_LIBADD  = $(ZSTD_LIBS)
