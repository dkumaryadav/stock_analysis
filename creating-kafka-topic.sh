#======================== Creating kafka topics

# Topic for telling data has been download from API and needs to be persisted to HBase
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic data-available

# Topic to publish the top 10 stocks of the day
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic top10

# Topic to publish the bottom 10 stocks of the day
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bottom10
