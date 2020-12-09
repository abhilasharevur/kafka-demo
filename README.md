# kafka python client

Python client for the Apache Kafka distributed stream processing system.
kafka-python is designed to function much like the official java client, with a
sprinkling of pythonic interfaces (e.g., consumer iterators).

See <https://kafka-python.readthedocs.io/en/master/compatibility.html>
for more details.

Please note that the master branch may contain unreleased features. For release
documentation, please see readthedocs and/or python's inline help.

```
>>> pip install kafka-python
```

See <https://github.com/dpkp/kafka-python/edit/master/README.rst> for examples.

## Requirement

```
>>> pip install psycopg2-binary
>>> pip install faker
```

## Producer

The producer is asynchronous which uses factories to put message to the kafka server 
created in Aiven console. Factory class produce fake json data using faker library. 
Producer can be safely used across threads. Each factory is like a single line to 
produce kafka messages. The topic is created on Aiven console and should be passed as 
an argument. A factory send data to the topic at kafka server.Factory collects metadata 
and calls callback function whether success or error. The thread is run for small time and stopped.
This can be controlled in the callback function.
See <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html> 
for more details.


### Run the kafka producer

1. Using command line

```
>>> ./main.py --bootstrap-servers localhost:9092 --topic test --ssl-cafile path/to/ca.pem 
 --ssl-keyfile path/to/service.key --ssl-certfile path/to/service.cert --security-protocol SSL --producer
 ```

2. Using .cfg file

Update configuration in producer.cfg file
```
>>> python -m DemoProducer.producer
```

## Consumer

Using the APIs mentioned in  <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html> 
and configuration details. Creating one consumer that collects data from kafka server using the credentials 
shown on the Aiven console. The consumer is provided topic which can be checked to see if it exists. 
To collect oldest data, config "auto_offset_reset=earliest". Consumer calls connect and insert function to connect
postgresql server. 

### Run the kafka consumer

1. Using command line

```
>>> ./main.py --bootstrap-servers localhost:9092 --topic test --ssl-cafile path/to/ca.pem 
 --ssl-keyfile path/to/service.key --ssl-certfile path/to/service.cert --security-protocol SSL --consumer
 ```
 
 2. Using .cfg file /config/consumer.cfg
 ```
 >>>python -m DemoConsumer.consumer
```

#### Check the postgresql database

```
>>> import psycopg2 as pc
>>> conn = pc.connect(uri)
>>> cursor = conn.cursor()
>>> cursor.execute("select current_database();")
>>> cursor.fetchone()
>>> s = "SELECT table_schema, table_name FROM information_schema.tables where (table_schema = 'public') order by table_schema, table_name"
>>> cursor.execute(s)
>>> cursor.fetchall()
# ('public', 'orders_03')
>>> cursor.execute("select * from orders_03 where product = 'Toy Train';")
>>> res = cursor.fetchall()
>>> len(res)
# 7226
```
