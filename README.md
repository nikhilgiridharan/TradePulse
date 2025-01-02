# High-Performance Stock Data Pipeline in C++

## Project Overview
This project implements a high-performance data pipeline for processing real-time stock market data. Using **C++**, **Apache Kafka**, and **MySQL**, the system captures, processes, and stores stock data efficiently with the following goals:

- **Throughput**: Handle 500,000 events/second.
- **Latency**: Sub-100 milliseconds.
- **Uptime**: 99%.

## Features
1. **Data Cleaning**: Preprocess raw stock data (e.g., standardizing dates, handling missing values).
2. **Kafka Integration**: Publish and consume stock data using Apache Kafka.
3. **Database Storage**: Store processed data in a MySQL database.
4. **Scalability**: Designed to handle high volumes of real-time data.

## Prerequisites
1. **C++ Compiler**: GCC or Clang supporting C++17.
2. **Apache Kafka**: Installed and running on `localhost:9092`.
3. **MySQL Database**: Installed and accessible.
4. **Libraries**:
   - [librdkafka](https://github.com/edenhill/librdkafka): Kafka client for C++.
   - [MySQL Connector/C++](https://dev.mysql.com/doc/connector-cpp/en/): MySQL database connectivity.

## Project Structure
```
.
├── data_cleaning.cpp        # Data preprocessing script
├── kafka_producer.cpp       # Kafka producer script
├── kafka_consumer.cpp       # Kafka consumer script
├── schema.sql               # MySQL database schema
├── stockData.csv            # Raw stock data
├── Makefile                 # Build script
├── README.md                # Documentation
```

## Setup and Usage

### Step 1: Install Dependencies

1. **Install Apache Kafka**:
   ```bash
   wget https://downloads.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz
   tar -xzf kafka_2.13-3.0.0.tgz
   cd kafka_2.13-3.0.0
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```

2. **Install MySQL**:
   ```bash
   sudo apt update
   sudo apt install mysql-server
   ```

3. **Install Libraries**:
   - Install librdkafka:
     ```bash
     sudo apt install librdkafka-dev
     ```
   - Install MySQL Connector:
     ```bash
     sudo apt install libmysqlcppconn-dev
     ```

### Step 2: Configure Kafka

Create the Kafka topic:
```bash
bin/kafka-topics.sh --create --topic stock_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Step 3: Set Up MySQL

Run the provided schema file to create the database and table:
```bash
mysql -u username -p < schema.sql
```
Replace `username` with your MySQL username.

### Step 4: Build the Project

Use the `Makefile` to compile all components:
```bash
make
```

### Step 5: Preprocess Data

Run the data cleaning script:
```bash
./data_cleaning
```
This will create a cleaned version of `stockData.csv` as `cleaned_stockData.csv`.

### Step 6: Produce Data

Start the Kafka producer to send cleaned data to the Kafka topic:
```bash
./kafka_producer
```

### Step 7: Consume and Store Data

Run the Kafka consumer to read data from Kafka and store it in MySQL:
```bash
./kafka_consumer
```

## File Details

### 1. `data_cleaning.cpp`
- Reads the raw `stockData.csv`.
- Standardizes date format to `YYYY-MM-DD`.
- Replaces zero volumes with `N/A`.
- Saves cleaned data to `cleaned_stockData.csv`.

### 2. `kafka_producer.cpp`
- Reads `cleaned_stockData.csv`.
- Publishes each record to the Kafka topic `stock_topic`.

### 3. `kafka_consumer.cpp`
- Consumes data from `stock_topic`.
- Parses the data and inserts it into the `StockPrices` table in MySQL.

### 4. `schema.sql`
- Creates a MySQL database `StockMarketDB` and a table `StockPrices` with the following schema:

```sql
CREATE DATABASE StockMarketDB;

USE StockMarketDB;

CREATE TABLE StockPrices (
    ID BIGINT AUTO_INCREMENT PRIMARY KEY,
    `Index` VARCHAR(10) NOT NULL,
    `Date` DATE NOT NULL,
    Open DECIMAL(10, 2) NOT NULL,
    High DECIMAL(10, 2) NOT NULL,
    Low DECIMAL(10, 2) NOT NULL,
    Close DECIMAL(10, 2) NOT NULL,
    AdjClose DECIMAL(10, 2) NOT NULL,
    Volume VARCHAR(20),
    CloseUSD DECIMAL(10, 2) NOT NULL
);
```

### 5. `Makefile`
- Automates the compilation of all C++ files.
- Usage:
  ```bash
  make         # Build all executables
  make clean   # Remove compiled files
  ```

## Testing
1. **Kafka Producer**:
   - Ensure messages are being published to the Kafka topic by using:
     ```bash
     bin/kafka-console-consumer.sh --topic stock_topic --from-beginning --bootstrap-server localhost:9092
     ```

2. **Kafka Consumer**:
   - Verify data is being inserted into MySQL by running:
     ```sql
     SELECT * FROM StockPrices;
     ```

## Troubleshooting
1. **Kafka Connection Errors**:
   - Verify Kafka is running and accessible on `localhost:9092`.

2. **MySQL Errors**:
   - Check database credentials and ensure the `StockMarketDB` schema exists.

3. **Compilation Issues**:
   - Ensure all required libraries (e.g., `librdkafka`, `libmysqlcppconn`) are installed and accessible.

## Future Enhancements
- Add multi-threading to improve consumer performance.
- Introduce data validation checks during cleaning.
- Support additional data sources (e.g., WebSocket APIs).

---

## Contributors
- **Nikhil Giridharan**

