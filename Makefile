PGFILEDESC = "pg_control_editor"
PGAPPICON = win32

PROGRAM = pg_control_editor
OBJS = \
	$(WIN32RES) \
	pg_control_editor.o

TAP_TESTS = 0

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LDFLAGS = -lpgfeutils
PG_LIBS_INTERNAL = $(libpq_pgport)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_control_editor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

