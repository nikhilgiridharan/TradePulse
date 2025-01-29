# TradePulse

## Project Overview
This project implements a high-performance data pipeline for processing real-time stock market data. Using **Python**, **Apache Kafka**, and **Cassandra**, the system captures, processes, and stores stock data efficiently with the following goals:

- **Throughput**: Handle 500,000 events/second.
- **Latency**: Sub-100 milliseconds.
- **Uptime**: 99%.

## Features
1. **Data Cleaning**: Preprocess raw stock data (e.g., standardizing dates, handling missing values).
2. **Kafka Integration**: Publish and consume stock data using Apache Kafka.
3. **Database Storage**: Store processed data in a Cassandra database.
4. **Scalability**: Designed to handle high volumes of real-time data.

## Prerequisites
1. **Python 3.8+**
2. **Apache Kafka**: Installed and running on `localhost:9092`.
3. **Cassandra Database**: Installed and accessible.
4. **Libraries**:
   - [pandas](https://pandas.pydata.org/): Data processing.
   - [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python): Kafka client for Python.
   - [cassandra-driver](https://github.com/datastax/python-driver): Cassandra database connectivity.

## Project Structure
```
.
├── data_cleaning.py        # Data preprocessing script
├── kafka_producer.py       # Kafka producer script
├── kafka_consumer.py       # Kafka consumer script
├── schema.cql              # Cassandra database schema
├── stockData.csv           # Raw stock data
├── requirements.txt        # Python dependencies
├── README.md               # Documentation
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

2. **Install Cassandra**:
   ```bash
   sudo apt update
   sudo apt install cassandra
   ```

3. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Step 2: Configure Kafka

Create the Kafka topic:
```bash
bin/kafka-topics.sh --create --topic stock_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Step 3: Set Up Cassandra

Run the provided schema file to create the database and table:
```bash
cqlsh -f schema.cql
```

### Step 4: Preprocess Data

Run the data cleaning script:
```bash
python data_cleaning.py
```
This will create a cleaned version of `stockData.csv` as `cleaned_stockData.csv`.

### Step 5: Produce Data

Start the Kafka producer to send cleaned data to the Kafka topic:
```bash
python kafka_producer.py
```

### Step 6: Consume and Store Data

Run the Kafka consumer to read data from Kafka and store it in Cassandra:
```bash
python kafka_consumer.py
```

## Functional Details

### Data Cleaning (`data_cleaning.py`)
- **Purpose**: Prepares raw stock data for further processing by standardizing formats and ensuring data integrity.
- **Key Features**:
  - **Date Standardization**: Converts dates from `DD/MM/YY` format to ISO 8601 format (`YYYY-MM-DD`).
  - **Volume Correction**: Replaces `0` values in the `Volume` column with `N/A` to avoid misleading results.
  - **Missing Data Handling**: Removes rows with missing values to ensure data completeness.
  - **Output**: Saves the cleaned data to a new file, `cleaned_stockData.csv`.

### Kafka Producer (`kafka_producer.py`)
- **Purpose**: Sends cleaned stock data to a Kafka topic for real-time processing.
- **Key Features**:
  - **File Reading**: Reads each row of the cleaned CSV file.
  - **Message Publishing**: Publishes each row as a CSV string to the Kafka topic `stock_topic`.
  - **Error Handling**: Ensures messages are successfully delivered and logs errors if they occur.
  - **Scalability**: Supports high-throughput publishing by batching messages (optional optimization).

### Kafka Consumer (`kafka_consumer.py`)
- **Purpose**: Consumes stock data from a Kafka topic and inserts it into a Cassandra database.
- **Key Features**:
  - **Message Consumption**: Reads messages from the Kafka topic `stock_topic` in real time.
  - **Data Parsing**: Parses each message (CSV format) into individual fields.
  - **Database Insertion**: Inserts the parsed data into the `stockprices` table in Cassandra.
  - **Error Handling**: Handles Kafka or Cassandra connection issues and retries operations as needed.

### Cassandra Database Schema (`schema.cql`)
- **Purpose**: Defines the structure of the `stockprices` table to store processed stock data.
- **Key Features**:
  - **Fields**:
    - `Index`: Stock index or ticker symbol (e.g., `AAPL`).
    - `Date`: The date of the stock data in `YYYY-MM-DD` format.
    - `Open`, `High`, `Low`, `Close`, `AdjClose`: Stock prices for the day.
    - `Volume`: Number of shares traded, stored as a string to handle potential `N/A` values.
    - `CloseUSD`: Closing price converted to USD.
  - **Primary Key**: (`Index`, `Date`) ensures unique identification of each record.

## Testing

### Kafka Producer
1. Start Kafka console consumer to verify message publishing:
   ```bash
   bin/kafka-console-consumer.sh --topic stock_topic --from-beginning --bootstrap-server localhost:9092
   ```
2. Check if messages from `kafka_producer.py` are visible in the Kafka topic.

### Kafka Consumer
1. Run `kafka_consumer.py` to consume messages and insert them into Cassandra.
2. Query the database:
   ```sql
   SELECT * FROM stockprices;
   ```
3. Verify the data matches the cleaned stock data.

## Troubleshooting
1. **Kafka Connection Errors**:
   - Ensure Kafka broker is running and accessible on `localhost:9092`.
   - Verify network configurations if running Kafka on a remote server.

2. **Cassandra Connection Errors**:
   - Check database credentials in `kafka_consumer.py`.
   - Ensure the database server is running and the schema is correctly applied.

3. **Dependency Issues**:
   - Ensure all required Python libraries are installed using `pip install -r requirements.txt`.

## Future Enhancements
- Add multi-threading to the Kafka consumer to increase processing speed.
- Include data validation logic to handle corrupted or malformed records.
- Integrate additional data sources (e.g., WebSocket APIs for real-time stock data).
- Optimize database writes using batch insertion techniques.

---

## Contributors
- **Nikhil Giridharan**

