python3 producer.py 

systemctl start kafka
systemctl status kafka
systemctl start zookeeper
systemctl status zookeeper

sudo mysql -u root -p


wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.33.tar.gz
tar -xzf mysql-connector-j-8.0.33.tar.gz
mv mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar ~/mysql-connector.jar

java -version


pip3 install pyspark==3.5.0 mysql-connector-python kafka-python
sudo apt install mysql-server -y
sudo mysql_secure_installation

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.kafka:kafka-clients:3.7.0,com.mysql:mysql-connector-j:9.0.0  streaming_processor.py

wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz -O ~/spark-3.5.5-bin-hadoop3.tgz
tar -xzf ~/spark-3.5.5-bin-hadoop3.tgz -C ~/
mv ~/spark-3.5.5-bin-hadoop3 ~/spark-3.5.5
spark-shell --version

echo "export SPARK_HOME=/home/musashi/spark-3.5.5" >> ~/.bashrc
echo "export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH" >> ~/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.bashrc


/usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic top1 --from-beginning


/usr/local/kafka/bin/kafka-topics.sh --create --topic top3 --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092


/usr/local/kafka/bin/kafka-consumer-groups.sh   --bootstrap-server localhost:9092   --group my-spark-consumer   --reset-offsets   --to-earliest   --topic top1   --topic top2   --topic top3   --execute

rm -rf /home/musashi/dbt_project/checkpoint/raw
rm -rf /home/musashi/dbt_project/checkpoint/agg

/usr/local/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

/usr/local/kafka/bin/kafka-topics.sh --describe --topic topic3 --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh --create --topic topic2 --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
