# 1. Start Kafka
docker-compose up -d

# 2. Run Kafka Engine (new terminal)
java -jar KafkaEngine/target/kafka-engine-1.0-SNAPSHOT.jar

# 3. Run Kafka Streams (new terminal)
java -jar KafkaStreams/target/kafka-streams-engine-1.0-SNAPSHOT.jar

# 4. Run Anomaly Detection (new terminal)
python AnomalyDetection/ConsumerApp.py
