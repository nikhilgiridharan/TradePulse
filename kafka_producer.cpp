#include <iostream>
#include <librdkafka/rdkafka.h>
#include <fstream>
#include <sstream>
#include <string>

void produceStockData(const std::string& filePath, const std::string& topicName) {
    rd_kafka_t* producer;
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char errstr[512];

    // Create Kafka producer
    producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return;
    }

    // Set broker
    if (rd_kafka_brokers_add(producer, "localhost:9092") == 0) {
        std::cerr << "Failed to add broker" << std::endl;
        rd_kafka_destroy(producer);
        return;
    }

    // Open stock data file
    std::ifstream inFile(filePath);
    std::string line;

    while (getline(inFile, line)) {
        // Send line to Kafka
        rd_kafka_produce(
            rd_kafka_topic_new(producer, topicName.c_str(), nullptr),
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            const_cast<char*>(line.c_str()), line.size(),
            nullptr, 0,
            nullptr
        );

        rd_kafka_flush(producer, 1000); // Ensure message delivery
    }

    inFile.close();
    rd_kafka_destroy(producer);
    std::cout << "Data published to Kafka topic: " << topicName << std::endl;
}

int main() {
    produceStockData("cleaned_stockData.csv", "stock_topic");
    return 0;
}
