
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g #-Ofast -march=native -mtune=native -fopenmp
CC = g++

all: correctness persistence my_correctness

correctness: kvstore.o correctness.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o

persistence: kvstore.o persistence.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o

my_correctness: kvstore.o my_correctness.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o

driver_test: driver.o kvstore.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o
	$(LINK.o) -o driver_test driver.o kvstore.o skip_list.o bloom_filter.o ss_table.o v_log.o logger.o
	
driver: driver_test
	./driver_test data data/vlog

logger.o: utils/logger.cc utils/logger.h
	$(CC) $(CXXFLAGS) -c utils/logger.cc 

driver.o: driver/driver.cc
	$(CC) $(CXXFLAGS) -c driver/driver.cc

clean:
	-rm -f correctness persistence my_correctness driver_test *.o
