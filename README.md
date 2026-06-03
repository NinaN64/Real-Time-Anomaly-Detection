# 0. Build the Project
mvn clean install

# 1. Start Kafka
docker-compose up -d

# 2. Run Kafka Engine (new terminal)
java -jar KafkaEngine/target/kafka-engine-1.0-SNAPSHOT.jar

# 3. Run Kafka Streams (new terminal)
java -jar KafkaStreams/target/kafka-streams-engine-1.0-SNAPSHOT.jar

# 4. Run Anomaly Detection (new terminal)
python AnomalyDetection/ConsumerApp.py

# 5. Run Evaluation (new terminal)
# This will match alerts with ground truth and append results to a CSV.
python AnomalyDetection/evaluator.py `
  --detector mmd `
  --trigger-n 100 `
  --window-type sliding `
  --source-topic preprocessed-batches-sliding

