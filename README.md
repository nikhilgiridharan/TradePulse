# TradePulse

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

## Functional Details

### Data Cleaning (`data_cleaning.cpp`)
- **Purpose**: Prepares raw stock data for further processing by standardizing formats and ensuring data integrity.
- **Key Features**:
  - **Date Standardization**: Converts dates from `DD/MM/YY` format to ISO 8601 format (`YYYY-MM-DD`).
  - **Volume Correction**: Replaces `0` values in the `Volume` column with `N/A` to avoid misleading results.
  - **Missing Data Handling**: Removes rows with missing values to ensure data completeness.
  - **Output**: Saves the cleaned data to a new file, `cleaned_stockData.csv`.

### Kafka Producer (`kafka_producer.cpp`)
- **Purpose**: Sends cleaned stock data to a Kafka topic for real-time processing.
- **Key Features**:
  - **File Reading**: Reads each row of the cleaned CSV file.
  - **Message Publishing**: Publishes each row as a JSON string to the Kafka topic `stock_topic`.
  - **Error Handling**: Ensures messages are successfully delivered and logs errors if they occur.
  - **Scalability**: Supports high-throughput publishing by batching messages (optional optimization).

### Kafka Consumer (`kafka_consumer.cpp`)
- **Purpose**: Consumes stock data from a Kafka topic and inserts it into a MySQL database.
- **Key Features**:
  - **Message Consumption**: Reads messages from the Kafka topic `stock_topic` in real time.
  - **Data Parsing**: Parses each message (CSV format) into individual fields.
  - **Database Insertion**: Inserts the parsed data into the `StockPrices` table in MySQL.
  - **Error Handling**: Handles Kafka or MySQL connection issues and retries operations as needed.

### MySQL Database Schema (`schema.sql`)
- **Purpose**: Defines the structure of the `StockPrices` table to store processed stock data.
- **Key Features**:
  - **Fields**:
    - `Index`: Stock index or ticker symbol (e.g., `AAPL`).
    - `Date`: The date of the stock data in `YYYY-MM-DD` format.
    - `Open`, `High`, `Low`, `Close`, `AdjClose`: Stock prices for the day.
    - `Volume`: Number of shares traded, stored as a string to handle potential `N/A` values.
    - `CloseUSD`: Closing price converted to USD.
  - **Primary Key**: `ID` ensures unique identification of each record.

### Makefile
- **Purpose**: Automates the compilation of project components.
- **Key Features**:
  - **Targets**:
    - `data_cleaning`: Compiles the data cleaning script.
    - `kafka_producer`: Compiles the Kafka producer script.
    - `kafka_consumer`: Compiles the Kafka consumer script.
  - **Clean**: Removes compiled executables to allow for fresh builds.

## Testing

### Kafka Producer
1. Start Kafka console consumer to verify message publishing:
   ```bash
   bin/kafka-console-consumer.sh --topic stock_topic --from-beginning --bootstrap-server localhost:9092
   ```
2. Check if messages from `kafka_producer` are visible in the Kafka topic.

### Kafka Consumer
1. Run `kafka_consumer` to consume messages and insert them into MySQL.
2. Query the database:
   ```sql
   SELECT * FROM StockPrices;
   ```
3. Verify the data matches the cleaned stock data.

## Troubleshooting
1. **Kafka Connection Errors**:
   - Ensure Kafka broker is running and accessible on `localhost:9092`.
   - Verify network configurations if running Kafka on a remote server.

2. **MySQL Connection Errors**:
   - Check database credentials in `kafka_consumer.cpp`.
   - Ensure the database server is running and the schema is correctly applied.

3. **Compilation Issues**:
   - Ensure `librdkafka` and `libmysqlcppconn` are installed and linked correctly.
   - Verify that the `Makefile` paths to libraries are accurate.

## Future Enhancements
- Add multi-threading to the Kafka consumer to increase processing speed.
- Include data validation logic to handle corrupted or malformed records.
- Integrate additional data sources (e.g., WebSocket APIs for real-time stock data).
- Optimize database writes using batch insertion techniques.

---

## Contributors
- **Nikhil Giridharan**

