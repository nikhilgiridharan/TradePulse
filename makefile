CXX = g++
CXXFLAGS = -std=c++17 -I/usr/local/include -L/usr/local/lib -lrdkafka -lmysqlcppconn
TARGETS = data_cleaning kafka_producer kafka_consumer

all: $(TARGETS)

data_cleaning: data_cleaning.cpp
	$(CXX) $(CXXFLAGS) -o data_cleaning data_cleaning.cpp

kafka_producer: kafka_producer.cpp
	$(CXX) $(CXXFLAGS) -o kafka_producer kafka_producer.cpp

kafka_consumer: kafka_consumer.cpp
	$(CXX) $(CXXFLAGS) -o kafka_consumer kafka_consumer.cpp

clean:
	rm -f $(TARGETS)
