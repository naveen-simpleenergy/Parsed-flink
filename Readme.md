# CAN Data Processing Pipeline with Apache Flink

![Apache Flink](https://img.shields.io/badge/Apache_Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)

Real-time processing pipeline for decoding and filtering CAN bus data using Apache Flink.

## 📌 Overview (version v1.0.0-rc.3)

A Flink-based streaming application that:
1. Consumes compressed CAN data from Kafka
2. Decodes raw CAN messages using DBC files
3. Filters non-fault signals
4. Restructures payloads
5. Produces processed data to downstream Kafka topics

## ✨ Features

- **Parallel Processing** matches Kafka topic partitions
- DBC File-based CAN message decoding
- Dynamic fault filtering with JSON configuration
- Kafka-to-Kafka stream processing
- Resource monitoring integration
- SASL authentication support for Kafka

## 🛠️ Prerequisites

- Python 3.7+
- Apache Flink 1.15+
- Apache Kafka 2.8+
- DBC file (provided in `./dbc_files`)
- Python packages:
