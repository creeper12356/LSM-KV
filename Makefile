
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g -Ofast
CC = g++

all: correctness persistence my_correctness

correctness: kvstore.o correctness.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o

persistence: kvstore.o persistence.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o

my_correctness: kvstore.o my_correctness.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o

logger.o: utils/logger.cc utils/logger.h
	$(CC) $(CXXFLAGS) -c utils/logger.cc -DENABLE_LOG

clean:
	-rm -f correctness persistence *.o
