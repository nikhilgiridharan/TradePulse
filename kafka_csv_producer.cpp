#include <librdkafka/rdkafkacpp.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <json/json.h>
#include <thread>
#include <iostream>
#include <chrono>

// Kafka Producer Configuration
const std::string KAFKA_BROKER = "<Your Public IP>:9092";
const std::string KAFKA_TOPIC = "demo_test";

// Function to Read CSV File
std::vector<std::map<std::string, std::string>> readCSV(const std::string &filePath) {
    std::vector<std::map<std::string, std::string>> rows;
    std::ifstream file(filePath);

    if (!file.is_open()) {
        throw std::runtime_error("Failed to open CSV file: " + filePath);
    }

    std::string line;
    std::vector<std::string> headers;

    // Read the header line
    if (std::getline(file, line)) {
        std::istringstream headerStream(line);
        std::string header;
        while (std::getline(headerStream, header, ',')) {
            headers.push_back(header);
        }
    }

    // Read the remaining lines
    while (std::getline(file, line)) {
        std::istringstream lineStream(line);
        std::string cell;
        std::map<std::string, std::string> row;
        for (const auto &header : headers) {
            if (std::getline(lineStream, cell, ',')) {
                row[header] = cell;
            }
        }
        rows.push_back(row);
    }

    file.close();
    return rows;
}

// Function to Convert Map to JSON String
std::string mapToJson(const std::map<std::string, std::string> &data) {
    Json::Value jsonData;
    for (const auto &pair : data) {
        jsonData[pair.first] = pair.second;
    }

    Json::StreamWriterBuilder writer;
    return Json::writeString(writer, jsonData);
}

int main() {
    // Kafka Producer Initialization
    std::string errstr;
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", KAFKA_BROKER, errstr);

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create Kafka producer: " << errstr << std::endl;
        return 1;
    }

    std::cout << "Kafka producer initialized successfully.\n";

    // Read CSV Data
    std::vector<std::map<std::string, std::string>> csvData;
    try {
        csvData = readCSV("stockData.csv");
        std::cout << "CSV file loaded successfully with " << csvData.size() << " rows.\n";
    } catch (const std::exception &e) {
        std::cerr << e.what() << "\n";
        delete producer;
        return 1;
    }

    // Send Data to Kafka
    try {
        while (true) {
            for (const auto &row : csvData) {
                std::string jsonMessage = mapToJson(row);
                RdKafka::ErrorCode resp = producer->produce(
                    KAFKA_TOPIC, RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                    const_cast<char *>(jsonMessage.c_str()), jsonMessage.size(),
                    nullptr, 0, 0, nullptr, nullptr);

                if (resp != RdKafka::ERR_NO_ERROR) {
                    std::cerr << "Failed to produce message: " << RdKafka::err2str(resp) << "\n";
                } else {
                    std::cout << "Message sent: " << jsonMessage << "\n";
                }

                // Flush producer queue
                producer->poll(0);

                // Sleep for 0.1 seconds
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    } catch (const std::exception &e) {
        std::cerr << "An error occurred while sending data to Kafka: " << e.what() << "\n";
    }

    // Flush and Cleanup
    try {
        producer->flush(5000);
    } catch (const std::exception &e) {
        std::cerr << "An error occurred while flushing the producer: " << e.what() << "\n";
    }

    delete producer;
    return 0;
}
