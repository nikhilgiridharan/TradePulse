from confluent_kafka import Consumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def save_to_cassandra(session, table, message):
    query = f"""
    INSERT INTO {table} (index, date, open, high, low, close, adj_close, volume, close_usd)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    statement = SimpleStatement(query)
    values = message.split(',')
    session.execute(statement, values)

def consume_stock_data(topic_name, broker="localhost:9092", cassandra_host="127.0.0.1"):
    cluster = Cluster([cassandra_host])
    session = cluster.connect()
    session.set_keyspace("stockmarketdb")
    
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'stock_consumer_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic_name])
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        save_to_cassandra(session, "stockprices", msg.value().decode('utf-8'))

# Usage
consume_stock_data("stock_topic")
