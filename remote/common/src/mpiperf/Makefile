CC=mpicc
CFLAGS=-g -Wall -Wextra -Werror -std=c99 -fPIC
LDFLAGS=-shared

all: mpiperf.so

mpiperf.so: mpiperf.o
	$(CC) $(LDFLAGS) -o $@ $+

clean:
	rm -f mpiperf.{o,so}
