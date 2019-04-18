
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
CFLAGS+=-DWITH_UTIL_LINUX=1
LDLIBS+=-lmount -lblkid
endif

ifneq ($(BUILD_SINGLE_BINARY),)
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
