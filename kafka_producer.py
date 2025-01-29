from confluent_kafka import Producer
import pandas as pd

def produce_stock_data(file_path, topic_name, broker="localhost:9092"):
    producer = Producer({'bootstrap.servers': broker})
    df = pd.read_csv(file_path)
    
    for _, row in df.iterrows():
        message = ','.join(map(str, row.values))
        producer.produce(topic_name, message)
        producer.flush()

    print(f"Data published to Kafka topic: {topic_name}")

# Usage
produce_stock_data("cleaned_stockData.csv", "stock_topic")
