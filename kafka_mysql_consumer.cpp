#include <librdkafka/rdkafkacpp.h>
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <json/json.h>
#include <iostream>
#include <string>

// Kafka Consumer Configuration
const std::string KAFKA_BROKER = "<Your Public IP>:9092";
const std::string KAFKA_TOPIC = "demo_test";

// MySQL Database Configuration
const std::string MYSQL_HOST = "tcp://<Your MySQL IP>:3306";
const std::string MYSQL_USER = "<Your MySQL Username>";
const std::string MYSQL_PASS = "<Your MySQL Password>";
const std::string MYSQL_DB = "stockmarket";

int main() {
    try {
        // Initialize Kafka Consumer
        std::string errstr;
        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        conf->set("bootstrap.servers", KAFKA_BROKER, errstr);

        RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
        if (!consumer) {
            std::cerr << "Failed to create Kafka consumer: " << errstr << std::endl;
            return 1;
        }

        consumer->subscribe({KAFKA_TOPIC});
        std::cout << "Kafka consumer subscribed to topic: " << KAFKA_TOPIC << std::endl;

        // Initialize MySQL Connection
        sql::mysql::MySQL_Driver *driver = sql::mysql::get_mysql_driver_instance();
        std::unique_ptr<sql::Connection> conn(driver->connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASS));
        conn->setSchema(MYSQL_DB);

        // Create Table if not Exists
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        stmt->execute(
            "CREATE TABLE IF NOT EXISTS stock_market_data ("
            "id INT PRIMARY KEY AUTO_INCREMENT, "
            "`index` VARCHAR(255), "
            "date VARCHAR(255), "
            "open FLOAT, "
            "high FLOAT, "
            "low FLOAT, "
            "close FLOAT, "
            "`adj_close` FLOAT, "
            "volume BIGINT, "
            "closeUSD FLOAT);");

        std::cout << "Connected to MySQL and table initialized.\n";

        // Consume Kafka Messages and Insert into MySQL
        int message_id = 0;
        while (true) {
            RdKafka::Message *msg = consumer->consume(1000);
            if (msg->err() == RdKafka::ERR_NO_ERROR) {
                std::string payload = static_cast<const char *>(msg->payload());
                Json::Value jsonData;
                Json::CharReaderBuilder readerBuilder;
                std::string errs;

                if (Json::parseFromStream(readerBuilder, std::istringstream(payload), &jsonData, &errs)) {
                    // Increment ID for each message
                    message_id++;

                    // Insert Data into MySQL
                    std::unique_ptr<sql::PreparedStatement> pstmt(
                        conn->prepareStatement(
                            "INSERT INTO stock_market_data (`index`, date, open, high, low, close, `adj_close`, volume, closeUSD) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"));

                    pstmt->setString(1, jsonData["index"].asString());
                    pstmt->setString(2, jsonData["date"].asString());
                    pstmt->setDouble(3, jsonData["open"].asFloat());
                    pstmt->setDouble(4, jsonData["high"].asFloat());
                    pstmt->setDouble(5, jsonData["low"].asFloat());
                    pstmt->setDouble(6, jsonData["close"].asFloat());
                    pstmt->setDouble(7, jsonData["adj_close"].asFloat());
                    pstmt->setInt64(8, jsonData["volume"].asLargestUInt());
                    pstmt->setDouble(9, jsonData["closeUSD"].asFloat());

                    pstmt->execute();
                    std::cout << "Inserted message ID: " << message_id << " into MySQL.\n";
                } else {
                    std::cerr << "Failed to parse JSON: " << errs << "\n";
                }
            } else if (msg->err() != RdKafka::ERR__TIMED_OUT) {
                std::cerr << "Kafka error: " << msg->errstr() << "\n";
            }
            delete msg;
        }

        // Cleanup
        consumer->close();
        delete consumer;

    } catch (const sql::SQLException &e) {
        std::cerr << "MySQL error: " << e.what() << "\n";
        return 1;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
