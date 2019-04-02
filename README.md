# rabbit-consumer
Simple consumer for rabbit mq

### Prerequisites
java 1.8+

maven 3.3.9+

docker


### Installing

Setup a local dockerized RabbitMQ cluster of 3 nodes https://dzone.com/articles/rabbitmq-in-cluster - nodes addresses: 192.168.99.100:30000,192.168.99.100:30002,192.168.99.100:30004

Create a durable eagerly synchronized mirrored queue (name=q.example) with a direct exchange and a route between (routingKey=example_key) them https://www.rabbitmq.com/ha.html

maven install

java -jar target/consumer-1.0-SNAPSHOT.jar



