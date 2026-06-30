# Real-Time Anomaly Detection

## First-Time Setup

### 1. Build the Java Project
```powershell
mvn clean install
```

### 2. Setup Python Virtual Environment
```powershell
py -m venv .venv
.venv\Scripts\pip install -r AnomalyDetection/requirements.txt
```

---

## Running the Project (Subsequent Runs)

### 1. Start Kafka
```powershell
docker-compose up -d
```

### 2. Run Kafka Engine (new terminal)
**20 Newsgroups (default):**
```powershell
java -jar KafkaEngine/target/kafka-engine-1.0-SNAPSHOT.jar
```
**Wikipedia:**
```powershell
java -jar KafkaEngine/target/kafka-engine-1.0-SNAPSHOT.jar --dataset wikipedia
```
**arxiv:**
```powershell
java -jar KafkaEngine/target/kafka-engine-1.0-SNAPSHOT.jar --dataset arxiv
```

java -jar KafkaEngine/target/kafka-engine-1.0-SNAPSHOT.jar --dataset yahoo --seed 7 --subset-size 2000


### 3. Run Kafka Streams (new terminal)
```powershell
java -jar KafkaStreams/target/kafka-streams-engine-1.0-SNAPSHOT.jar
```
 
### 4. Run Anomaly Detection (new terminal)
**N = 100 (default):**
```powershell
.venv\Scripts\python.exe AnomalyDetection/ConsumerApp.py
```
**N = 50:**
```powershell
.venv\Scripts\python.exe AnomalyDetection/ConsumerApp.py --trigger-n 50
```
**N = 200:**
```powershell
.venv\Scripts\python.exe AnomalyDetection/ConsumerApp.py --trigger-n 200
```

**Noise Warmup:**
```powershell 
.venv\Scripts\python.exe AnomalyDetection/ConsumerApp.py --noise-warmup
```

### 5. Run Evaluation (new terminal)

* **To evaluate the MMD detector:**
  ```powershell
  .venv\Scripts\python.exe AnomalyDetection/evaluator.py `
    --detector mmd `
    --trigger-n 100 `
    --window-type sliding `
    --source-topic preprocessed-batches-sliding `
    --dataset yahoo
  ```

* **To evaluate the PADD detector:**
  ```powershell
  .venv\Scripts\python.exe AnomalyDetection/evaluator.py `
    --detector padd `
    --trigger-n 100 `
    --window-type sliding `
    --source-topic preprocessed-batches-sliding `
    --dataset yahoo
  ```

### 6. Analyze results
python analyze_results.py --input evaluation_results.csv --output stats_summary.csv --by-dataset