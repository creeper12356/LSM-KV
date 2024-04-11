
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g

all: correctness persistence

correctness: kvstore.o correctness.o skip_list.o bloom_filter.o ss_table.o v_log.o

persistence: kvstore.o persistence.o skip_list.o bloom_filter.o ss_table.o v_log.o

clean:
	-rm -f correctness persistence *.o data/level-0/*
