.PHONY = install clean

PG_CONFIG = pg_config
PKGLIBDIR = $(shell $(PG_CONFIG) --pkglibdir)
CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)
CFLAGS += -fPIC

dummy_validator.so: dummy_validator.o
	$(CC) -shared $(CFLAGS) $(LDFLAGS) -o $@ $^

dummy_validator.o: dummy_validator.c

install:
	install -D -m 755 dummy_validator.so $(PKGLIBDIR)/

clean:
	rm -f dummy_validator.o dummy_validator.so
