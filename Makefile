LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall -g -Ofast -march=native -mtune=native -fopenmp
ifeq ($(ENABLE_LOG), 1)
	CXXFLAGS += -DENABLE_LOG
endif
CC = g++

OBJS = kvstore.o skip_list.o bloom_filter.o ss_table.o ss_table_manager.o v_log.o logger.o

all: correctness persistence performance

correctness: $(OBJS) correctness.o
persistence: $(OBJS) persistence.o
my_correctness: $(OBJS) my_correctness.o
performance: $(OBJS) performance.o

%.o: %.cc %.h
	$(CC) $(CXXFLAGS) -c $<

logger.o: utils/logger.cc utils/logger.h
	$(CC) $(CXXFLAGS) -c $<

performance.o: test/performance.cc
	$(CC) $(CXXFLAGS) -c $<


clean:
	-rm -f correctness persistence performance *.o