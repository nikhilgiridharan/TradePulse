
High-Performance Stock Data Pipeline in C++

Project Overview

This project implements a high-performance data pipeline for processing real-time stock market data. Using C++, Apache Kafka, and MySQL, the system captures, processes, and stores stock data efficiently with the following goals:

Throughput: Handle 500,000 events/second.

Latency: Sub-100 milliseconds.

Uptime: 99%.

Features

Data Cleaning: Preprocess raw stock data (e.g., standardizing dates, handling missing values).

Kafka Integration: Publish and consume stock data using Apache Kafka.

Database Storage: Store processed data in a MySQL database.

Scalability: Designed to handle high volumes of real-time data.

Prerequisites

C++ Compiler: GCC or Clang supporting C++17.

Apache Kafka: Installed and running on localhost:9092.

MySQL Database: Installed and accessible.

Libraries:

librdkafka: Kafka client for C++.

MySQL Connector/C++: MySQL database connectivity.
