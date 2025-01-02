#include <iostream>
#include <librdkafka/rdkafka.h>
#include <string>
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/prepared_statement.h>

// Function to save stock data to MySQL database
void saveToDatabase(const std::string& message) {
    sql::mysql::MySQL_Driver* driver = sql::mysql::get_mysql_driver_instance();
    std::unique_ptr<sql::Connection> conn(driver->connect("tcp://127.0.0.1:3306", "username", "password"));
    conn->setSchema("StockMarketDB");

    std::unique_ptr<sql::PreparedStatement> pstmt(
        conn->prepareStatement("INSERT INTO StockPrices (Index, Date, Open, High, Low, Close, AdjClose, Volume, CloseUSD) "
                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
    );

    std::stringstream ss(message);
    std::string token;

    // Assuming CSV format: Index,Date,Open,High,Low,Close,AdjClose,Volume,CloseUSD
    getline(ss, token, ','); pstmt->setString(1, token);
    getline(ss, token, ','); pstmt->setString(2, token);
    getline(ss, token, ','); pstmt->setDouble(3, std::stod(token));
    getline(ss, token, ','); pstmt->setDouble(4, std::stod(token));
    getline(ss, token, ','); pstmt->setDouble(5, std::stod(token));
    getline(ss, token, ','); pstmt->setDouble(6, std::stod(token));
    getline(ss, token, ','); pstmt->setDouble(7, std::stod(token));
    getline(ss, token, ','); pstmt->setString(8, token); // Assuming volume as string
    getline(ss, token, ','); pstmt->setDouble(9, std::stod(token));

    pstmt->executeUpdate();
    std::cout << "Data saved to database: " << message << std::endl;
}

// Function to consume stock data from Kafka
void consumeStockData(const std::string& topicName) {
    rd_kafka_t* consumer;
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char errstr[512];

    // Create Kafka consumer
    consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return;
    }

    rd_kafka_subscribe(consumer, rd_kafka_topic_partition_list_add(rd_kafka_topic_partition_list_new(1), topicName.c_str(), RD_KAFKA_PARTITION_UA));

    while (true) {
        rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer, 1000); // 1-second timeout
        if (msg) {
            if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                std::string message(static_cast<char*>(msg->payload), msg->len);
                saveToDatabase(message); // Save to database
            } else {
                std::cerr << "Consumer error: " << rd_kafka_message_errstr(msg) << std::endl;
            }
            rd_kafka_message_destroy(msg);
        }
    }

    rd_kafka_destroy(consumer);
}

int main() {
    consumeStockData("stock_topic");
    return 0;
}
