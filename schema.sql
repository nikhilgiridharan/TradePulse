CREATE KEYSPACE stockmarketdb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE stockmarketdb.stockprices (
    index text,
    date text,
    open double,
    high double,
    low double,
    close double,
    adj_close double,
    volume text,
    close_usd double,
    PRIMARY KEY (index, date)
);
