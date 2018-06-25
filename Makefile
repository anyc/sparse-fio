
app=sparse-fio

CFLAGS+=-g -Wall -D_FILE_OFFSET_BITS=64

all: $(app)

$(app): $(app).c

clean:
	rm -f $(app)
