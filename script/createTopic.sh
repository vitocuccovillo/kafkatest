cd ~/mykafka/kafka_2.11-2.4.1/bin/

./kafka-topics.sh --zookeeper localhost:2181 --create --topic first --partitions 2 --replication-factor 2