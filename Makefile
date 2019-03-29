
app=sparse-fio

CFLAGS+=-g -Wall -D_FILE_OFFSET_BITS=64

ifeq ($(NO_BENCHMARK),1)
CFLAGS+=-DNO_BENCHMARK
endif

ifneq ($(WITH_UTIL_LINUX),0)
CFLAGS+=-DWITH_UTIL_LINUX=1
LDLIBS+=-lmount -lblkid
endif

all: $(app)

$(app): $(app).c

clean:
	rm -f $(app)
