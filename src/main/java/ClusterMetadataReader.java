import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

class TopicMetadata {
    String name;
    UUID topicId;
    List<PartitionMetadata> partitions = new ArrayList<>();
    
    public TopicMetadata(String name, UUID topicId) {
        this.name = name;
        this.topicId = topicId;
    }
}

class PartitionMetadata {
    int partitionIndex;
    int leaderId;
    int leaderEpoch;
    List<Integer> isr = new ArrayList<>();
    
    public PartitionMetadata(int partitionIndex, int leaderId, int leaderEpoch) {
        this.partitionIndex = partitionIndex;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
    }
}

class ClusterMetadataReader {
    private static final String METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    private Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
    
    public ClusterMetadataReader() {
        try {
            readMetadataLog();
        } catch (Exception e) {
            System.err.println("Error reading cluster metadata: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void readMetadataLog() throws IOException {
        Path logPath = Paths.get(METADATA_LOG_PATH);
        if (!Files.exists(logPath)) {
            System.err.println("Metadata log file not found: " + METADATA_LOG_PATH);
            return;
        }
        
        try (FileChannel channel = FileChannel.open(logPath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            channel.read(buffer);
            buffer.flip();
            
            while (buffer.hasRemaining()) {
                // Try to read a record batch
                if (buffer.remaining() < 8) break; // Need at least 8 bytes for baseOffset
                
                long baseOffset = buffer.getLong();
                if (buffer.remaining() < 4) break; // Need at least 4 bytes for batchLength
                
                int batchLength = buffer.getInt();
                if (buffer.remaining() < batchLength) break; // Not enough data for the batch
                
                // Create a slice for this batch
                ByteBuffer batchBuffer = buffer.slice();
                batchBuffer.limit(batchLength);
                buffer.position(buffer.position() + batchLength);
                
                // Process the batch
                processBatch(batchBuffer);
            }
        }
        
        System.err.println("Loaded metadata for " + topicMetadataMap.size() + " topics");
        for (Map.Entry<String, TopicMetadata> entry : topicMetadataMap.entrySet()) {
            System.err.println("Topic: " + entry.getKey() + ", UUID: " + entry.getValue().topicId + 
                              ", Partitions: " + entry.getValue().partitions.size());
        }
    }
    
    private void processBatch(ByteBuffer buffer) {
        try {
            // Skip partitionLeaderEpoch (4 bytes)
            if (buffer.remaining() < 4) return;
            buffer.position(buffer.position() + 4);
            
            // Read magic byte
            if (buffer.remaining() < 1) return;
            byte magic = buffer.get();
            if (magic != 2) {
                System.err.println("Unsupported magic byte: " + magic);
                return;
            }
            
            // Skip CRC (4 bytes)
            if (buffer.remaining() < 4) return;
            buffer.position(buffer.position() + 4);
            
            // Skip attributes (2 bytes)
            if (buffer.remaining() < 2) return;
            buffer.position(buffer.position() + 2);
            
            // Skip lastOffsetDelta (4 bytes)
            if (buffer.remaining() < 4) return;
            buffer.position(buffer.position() + 4);
            
            // Skip firstTimestamp (8 bytes)
            if (buffer.remaining() < 8) return;
            buffer.position(buffer.position() + 8);
            
            // Skip maxTimestamp (8 bytes)
            if (buffer.remaining() < 8) return;
            buffer.position(buffer.position() + 8);
            
            // Skip producerId (8 bytes)
            if (buffer.remaining() < 8) return;
            buffer.position(buffer.position() + 8);
            
            // Skip producerEpoch (2 bytes)
            if (buffer.remaining() < 2) return;
            buffer.position(buffer.position() + 2);
            
            // Skip baseSequence (4 bytes)
            if (buffer.remaining() < 4) return;
            buffer.position(buffer.position() + 4);
            
            // Read records count
            if (buffer.remaining() < 4) return;
            int recordsCount = buffer.getInt();
            
            // Process each record in the batch
            for (int i = 0; i < recordsCount; i++) {
                processRecord(buffer);
            }
        } catch (Exception e) {
            System.err.println("Error processing batch: " + e.getMessage());
        }
    }
    
    private void processRecord(ByteBuffer buffer) {
        try {
            // Skip record length and attributes
            if (buffer.remaining() < 1) return;
            int recordHeaderSize = buffer.get() & 0xFF;
            if (buffer.remaining() < recordHeaderSize) return;
            buffer.position(buffer.position() + recordHeaderSize);
            
            // Read key length
            if (buffer.remaining() < 4) return;
            int keyLength = buffer.getInt();
            if (keyLength <= 0 || buffer.remaining() < keyLength) return;
            
            // Read key
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            
            // Read value length
            if (buffer.remaining() < 4) return;
            int valueLength = buffer.getInt();
            if (valueLength < 0 || buffer.remaining() < valueLength) return;
            
            // Read value
            byte[] valueBytes = new byte[valueLength];
            if (valueLength > 0) {
                buffer.get(valueBytes);
            }
            
            // Process key and value
            processMetadataRecord(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
    
    private void processMetadataRecord(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
        try {
            // Check if this is a topic metadata record
            if (keyBuffer.remaining() < 4) return;
            short keyVersion = keyBuffer.getShort();
            short resourceType = keyBuffer.getShort();
            
            // ResourceType 1 is for topics
            if (resourceType == 1) {
                // Read topic name
                if (keyBuffer.remaining() < 2) return;
                short nameLength = keyBuffer.getShort();
                if (nameLength <= 0 || keyBuffer.remaining() < nameLength) return;
                
                byte[] nameBytes = new byte[nameLength];
                keyBuffer.get(nameBytes);
                String topicName = new String(nameBytes, "UTF-8");
                
                // Skip if value is empty (tombstone record)
                if (valueBuffer.remaining() <= 0) return;
                
                // Read value
                if (valueBuffer.remaining() < 2) return;
                short valueVersion = valueBuffer.getShort();
                
                // Read topic ID (UUID)
                if (valueBuffer.remaining() < 16) return;
                long mostSigBits = valueBuffer.getLong();
                long leastSigBits = valueBuffer.getLong();
                UUID topicId = new UUID(mostSigBits, leastSigBits);
                
                // Read number of partitions
                if (valueBuffer.remaining() < 4) return;
                int numPartitions = valueBuffer.getInt();
                
                // Create topic metadata
                TopicMetadata topicMetadata = new TopicMetadata(topicName, topicId);
                
                // For simplicity, we'll create a partition with index 0
                if (numPartitions > 0) {
                    PartitionMetadata partitionMetadata = new PartitionMetadata(0, 0, 0);
                    partitionMetadata.isr.add(0); // Add broker 0 to ISR
                    topicMetadata.partitions.add(partitionMetadata);
                }
                
                // Store the topic metadata
                topicMetadataMap.put(topicName, topicMetadata);
                System.err.println("Found topic: " + topicName + " with ID: " + topicId);
            }
        } catch (Exception e) {
            System.err.println("Error processing metadata record: " + e.getMessage());
        }
    }
    
    public TopicMetadata getTopicMetadata(String topicName) {
        return topicMetadataMap.get(topicName);
    }
    
    public boolean topicExists(String topicName) {
        return topicMetadataMap.containsKey(topicName);
    }
}
