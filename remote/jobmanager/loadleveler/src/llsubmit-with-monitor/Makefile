LOADL=/opt/ibmll/LoadL/full
CC=xlc
CFLAGS=-q64 -I$(LOADL)/include -O5
LDFLAGS=-q64 -s -L$(LOADL)/lib -lllapi -O5

all: llsubmit-with-monitor

llsubmit-with-monitor: llsubmit-with-monitor.o
	$(CC) $(LDFLAGS) $(LIBS) -o $@ $+

clean:
	rm -f llmonitor{,.o}

.PHONY: all clean
