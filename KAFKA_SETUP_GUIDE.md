# Kafka-like Application - Complete Setup Guide (In-Memory Version)

## Overview
This is a full working **in-memory** Kafka-like distributed message broker application built with Spring Boot. It includes:
- **Producer API**: Send messages via REST API from frontend
- **Consumer API**: Read messages from the broker
- **Broker Server**: Stores messages in memory (no disk persistence)
- **Web UI**: User-friendly interface to send and consume messages

---

## Key Changes for In-Memory Storage

### Storage Mechanism:
- **Before**: Messages stored in `data/{topic}/partition-{n}.log` files
- **Now**: Messages stored in memory using `InMemoryLog` class
- **Benefits**: Faster access, no disk I/O, volatile storage (lost on restart)

### Components Modified:

1. **`InMemoryLog.java`** (NEW)
   - Thread-safe message storage using `ArrayList<String>`
   - Atomic offset management
   - Offset-based message retrieval

2. **`LogManager.java`** (UPDATED)
   - Now manages `InMemoryLog` instances instead of file logs
   - Thread-safe concurrent access

3. **`RequestHandler.java`** (UPDATED)
   - Writes messages to in-memory logs instead of files
   - No file system operations

4. **`ConsumerController.java`** (UPDATED)
   - Reads messages from in-memory logs
   - Added clear endpoints for testing

---

## Setup Instructions

### Prerequisites:
- Java 21
- Maven (or use mvnw.cmd)

### Step 1: Build the Project
```bash
cd C:\MyProjects\KafkaProject
mvnw.cmd clean install
```

### Step 2: Run the Application
```bash
mvnw.cmd spring-boot:run
```

The application will:
- Start the broker on port 9092 (automatically in background)
- Start Spring Boot on port 8080
- Wait for requests

**Expected Output:**
```
Broker startup initiated in background thread
Started KafkaProjectApplication in ... seconds
```

### Step 3: Access the Web UI
Open your browser and go to:
```
http://localhost:8080/
```

---

## API Documentation

### Producer API

#### Send Message
**Endpoint:** `POST /api/producer/send`

**Request Body:**
```json
{
  "topic": "orders",
  "key": "id123",
  "value": "order-data-here"
}
```

**Response:**
```json
{
  "topic": "orders",
  "partition": 1,
  "offset": 5
}
```

#### Health Check
**Endpoint:** `GET /api/producer/health`

**Response:**
```
Producer API is running. Send POST requests to /api/producer/send
```

---

### Consumer API

#### Get All Topics
**Endpoint:** `GET /api/consumer/topics`

**Response:**
```json
["orders", "events", "logs"]
```

#### Get Partitions of a Topic
**Endpoint:** `GET /api/consumer/topics/{topic}/partitions`

**Response:**
```json
[0, 1, 2]
```

#### Get Messages from a Partition (with offset support)
**Endpoint:** `GET /api/consumer/topics/{topic}/partition/{partition}/messages?offset=0&limit=100`

**Response:**
```json
[
  "order-1",
  "order-2",
  "order-3"
]
```

#### Get All Messages from a Topic
**Endpoint:** `GET /api/consumer/topics/{topic}/messages?limit=100`

**Response:**
```json
[
  "order-1",
  "order-2",
  "order-3"
]
```

#### Clear a Topic (DELETE all messages)
**Endpoint:** `DELETE /api/consumer/topics/{topic}`

**Response:**
```
Topic 'orders' cleared
```

#### Clear All Topics
**Endpoint:** `DELETE /api/consumer/all`

**Response:**
```
All topics cleared
```

---

## Example Workflow

### 1. Send Messages
```bash
curl -X POST http://localhost:8080/api/producer/send \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "key": "order-001", "value": "New order placed"}'
```

### 2. Read Messages
```bash
# Get all topics
curl http://localhost:8080/api/consumer/topics

# Get messages from topic
curl http://localhost:8080/api/consumer/topics/orders/messages

# Get messages from specific partition starting from offset 0
curl "http://localhost:8080/api/consumer/topics/orders/partition/0/messages?offset=0&limit=10"
```

### 3. Clear Data (for testing)
```bash
# Clear specific topic
curl -X DELETE http://localhost:8080/api/consumer/topics/orders

# Clear all topics
curl -X DELETE http://localhost:8080/api/consumer/all
```

---

## In-Memory vs File-Based Comparison

| Feature | In-Memory | File-Based |
|---------|-----------|------------|
| **Speed** | ⚡ Very Fast | 🐌 Slower (disk I/O) |
| **Persistence** | ❌ Lost on restart | ✅ Persistent |
| **Memory Usage** | 📈 High (all messages in RAM) | 📉 Low (only recent messages) |
| **Scalability** | 🔴 Limited by RAM | 🟢 Can handle large volumes |
| **Use Case** | Testing, development | Production, large data |

---

## Performance Characteristics

### In-Memory Advantages:
- **Sub-millisecond** message retrieval
- **No disk I/O** overhead
- **Thread-safe** concurrent access
- **Offset-based** reading support

### Limitations:
- **Memory bound**: Limited by available RAM
- **No persistence**: Data lost on application restart
- **No replication**: Single point of failure

---

## Memory Management

### Automatic Cleanup:
- Messages stored in `ArrayList<String>` per partition
- Thread-safe with `synchronized` methods
- Offset tracking with `AtomicLong`

### Monitoring Memory Usage:
```java
// In ConsumerController, you can add:
@GetMapping("/stats")
public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<>();
    Map<String, InMemoryLog> allLogs = logManager.getTopicLogs("");
    stats.put("totalTopics", getTopics().size());
    stats.put("totalPartitions", allLogs.size());

    long totalMessages = 0;
    for (InMemoryLog log : allLogs.values()) {
        totalMessages += log.getCurrentOffset();
    }
    stats.put("totalMessages", totalMessages);

    return stats;
}
```

---

## Converting Back to File-Based (Optional)

If you want to revert to file-based storage:

1. **Replace `InMemoryLog`** with original `Log` class
2. **Update `LogManager`** to use file paths
3. **Update `RequestHandler`** to write to files
4. **Update `ConsumerController`** to read from files

The file-based version is available in the git history or can be recreated using the original `Log.java`.

---

## Testing the In-Memory Implementation

### Load Testing:
```bash
# Send 1000 messages
for i in {1..1000}; do
  curl -X POST http://localhost:8080/api/producer/send \
    -H "Content-Type: application/json" \
    -d "{\"topic\": \"test\", \"key\": \"key-$i\", \"value\": \"message-$i\"}" \
    -s > /dev/null &
done
```

### Verify Storage:
```bash
# Check message count
curl http://localhost:8080/api/consumer/topics/test/messages | jq '. | length'
```

---

## Architecture Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Frontend  │───▶│ Producer API    │───▶│   Broker        │
│   (Port 8080)   │    │ (REST)          │    │   (Port 9092)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Consumer API    │◀───│ InMemoryLog     │
                       │ (REST)          │    │ (RAM Storage)   │
                       └─────────────────┘    └─────────────────┘
```

---

## Future Enhancements

- [ ] Add message TTL (time-to-live)
- [ ] Implement LRU cache for memory management
- [ ] Add message compression
- [ ] Implement consumer groups
- [ ] Add metrics and monitoring
- [ ] Persistent snapshots for faster recovery

---

## Troubleshooting

### Out of Memory Errors:
**Solution:** Clear topics periodically or increase JVM heap size:
```bash
mvnw.cmd spring-boot:run -Dspring-boot.run.jvmArguments="-Xmx2g"
```

### Messages Not Appearing:
**Solution:** Check that broker is running and messages are being sent to correct topic/partition.

### Performance Issues:
**Solution:** Monitor memory usage and clear old topics when not needed.

---

This in-memory version is perfect for **development**, **testing**, and **demonstration** purposes where speed is more important than persistence.
