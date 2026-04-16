# 1. Start Kafka
docker-compose up -d

# 2. Run Kafka Engine (new terminal)
cd KafkaEngine
java -jar target/kafka-engine-1.0-SNAPSHOT.jar

# 3. Run Kafka Streams (new terminal)
cd KafkaStreams
java -jar target/kafka-streams-engine-1.0-SNAPSHOT.jar

# 4. Run Anomaly Detection (new terminal)
cd AnomalyDetection
python3 ConsumerApp.py
# Real-Time-Anomaly-Detection
