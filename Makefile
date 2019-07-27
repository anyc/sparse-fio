
PKG_CONFIG?=pkg-config
INSTALL?=install
prefix?=/usr/local
bindir?=$(prefix)/bin
libdir?=$(prefix)/lib
includedir?=$(prefix)/include/

app=sparse-fio
lib=libsfio.so

CFLAGS+=-D_FILE_OFFSET_BITS=64

ifeq ($(DEBUG),1)
CFLAGS+=-g -Wall
endif

ifeq ($(NO_BENCHMARK),1)
CFLAGS+=-DNO_BENCHMARK
endif

ifneq ($(WITH_UTIL_LINUX),0)
CFLAGS+=-DWITH_UTIL_LINUX=1 $(shell $(PKG_CONFIG) --cflags mount blkid)
LDLIBS+=$(shell $(PKG_CONFIG) --libs mount blkid)
endif

ifneq ($(BUILD_SINGLE_EXECUTABLE),)
CFLAGS_APP=-DBUILD_APP_ONLY
LDFLAGS_APP=-L.
LDLIBS_APP=-lsfio
DEPS_APP=$(lib)

CFLAGS_LIB=-fPIC -DBUILD_LIB_ONLY

TARGETS=$(lib) $(app)
else
TARGETS=$(app)
endif

all: $(TARGETS)

$(app).app.o: CFLAGS+=$(CFLAGS_APP)
$(app).app.o: $(app).c
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

$(app).lib.o: CFLAGS+=$(CFLAGS_LIB)
$(app).lib.o: $(app).c
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@
	
$(app): LDLIBS+=$(LDLIBS_APP)
$(app): LDFLAGS+=$(LDFLAGS_APP)
$(app): $(app).app.o $(DEPS_APP)
	$(CC) $(LDFLAGS) $< $(LOADLIBES) $(LDLIBS) -o $@

$(lib): $(app).lib.o
	$(CC) -shared $(LDFLAGS) $< $(LOADLIBES) $(LDLIBS) -o $@

clean:
	rm -f *.o $(app) $(lib)

install:
	$(INSTALL) -m 755 -d $(DESTDIR)$(bindir)
	$(INSTALL) -m 755 -d $(DESTDIR)$(libdir)
	$(INSTALL) -m 755 -d $(DESTDIR)$(includedir)

	$(INSTALL) -m 755 $(app) $(DESTDIR)$(bindir)
	ln -s $(app) $(DESTDIR)$(bindir)/sfio
	$(INSTALL) -m 755 $(lib) $(DESTDIR)$(libdir)
	$(INSTALL) -m 644 sparse-fio.h $(DESTDIR)$(includedir)
