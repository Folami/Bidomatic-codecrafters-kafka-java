import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

// Helper classes from previous prompt, with fixes
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
    private final Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
    
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
                if (buffer.remaining() < 12) break; // Need 8 (baseOffset) + 4 (batchLength)
                
                long baseOffset = buffer.getLong();
                int batchLength = buffer.getInt();
                if (buffer.remaining() < batchLength) break;
                
                ByteBuffer batchBuffer = buffer.slice();
                batchBuffer.limit(batchLength);
                buffer.position(buffer.position() + batchLength);
                
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
            if (buffer.remaining() < 29) return; // Minimum batch header size
            buffer.position(buffer.position() + 4); // Skip partitionLeaderEpoch
            byte magic = buffer.get();
            if (magic != 2) {
                System.err.println("Unsupported magic byte: " + magic);
                return;
            }
            buffer.position(buffer.position() + 4); // Skip crc
            buffer.position(buffer.position() + 2); // Skip attributes
            buffer.position(buffer.position() + 4); // Skip lastOffsetDelta
            buffer.position(buffer.position() + 8); // Skip firstTimestamp
            buffer.position(buffer.position() + 8); // Skip maxTimestamp
            buffer.position(buffer.position() + 8); // Skip producerId
            buffer.position(buffer.position() + 2); // Skip producerEpoch
            buffer.position(buffer.position() + 4); // Skip baseSequence
            
            int recordsCount = buffer.getInt();
            for (int i = 0; i < recordsCount && buffer.hasRemaining(); i++) {
                processRecord(buffer);
            }
        } catch (Exception e) {
            System.err.println("Error processing batch: " + e.getMessage());
        }
    }
    
    private int readVarInt(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 28) {
                throw new RuntimeException("Invalid VARINT: too many bytes");
            }
        }
        throw new RuntimeException("Incomplete VARINT");
    }
    
    private void processRecord(ByteBuffer buffer) {
        try {
            if (buffer.remaining() < 1) return;
            int recordLength = readVarInt(buffer);
            if (buffer.remaining() < recordLength) return;
            
            int startPos = buffer.position();
            int attributes = readVarInt(buffer);
            readVarInt(buffer); // timestampDelta
            readVarInt(buffer); // offsetDelta
            
            int keyLength = readVarInt(buffer);
            if (keyLength < 0 || buffer.remaining() < keyLength) return;
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            
            int valueLength = readVarInt(buffer);
            byte[] valueBytes;
            if (valueLength >= 0) {
                if (buffer.remaining() < valueLength) return;
                valueBytes = new byte[valueLength];
                buffer.get(valueBytes);
            } else {
                valueBytes = new byte[0];
            }
            
            buffer.position(startPos + recordLength);
            processMetadataRecord(ByteBuffer.wrap(keyBytes), ByteBuffer.wrap(valueBytes));
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
    
    private void processMetadataRecord(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
        try {
            if (keyBuffer.remaining() < 4) return;
            short keyVersion = keyBuffer.getShort();
            short resourceType = keyBuffer.getShort();
            
            if (resourceType == 1) { // Topic
                if (keyBuffer.remaining() < 2) return;
                short nameLength = keyBuffer.getShort();
                if (nameLength <= 0 || keyBuffer.remaining() < nameLength) return;
                
                byte[] nameBytes = new byte[nameLength];
                keyBuffer.get(nameBytes);
                String topicName = new String(nameBytes, StandardCharsets.UTF_8);
                
                if (valueBuffer.remaining() <= 0) return; // Tombstone
                
                if (valueBuffer.remaining() < 22) return;
                short valueVersion = valueBuffer.getShort();
                long mostSigBits = valueBuffer.getLong();
                long leastSigBits = valueBuffer.getLong();
                UUID topicId = new UUID(mostSigBits, leastSigBits);
                int numPartitions = valueBuffer.getInt();
                
                TopicMetadata topicMetadata = new TopicMetadata(topicName, topicId);
                if (numPartitions > 0) {
                    PartitionMetadata partition = new PartitionMetadata(0, 0, 0);
                    partition.isr.add(0);
                    topicMetadata.partitions.add(partition);
                }
                
                synchronized (topicMetadataMap) {
                    topicMetadataMap.put(topicName, topicMetadata);
                }
                System.err.println("Found topic: " + topicName + " with ID: " + topicId);
            }
        } catch (Exception e) {
            System.err.println("Error processing metadata record: " + e.getMessage());
        }
    }
    
    public synchronized TopicMetadata getTopicMetadata(String topicName) {
        return topicMetadataMap.get(topicName);
    }
    
    public synchronized boolean topicExists(String topicName) {
        return topicMetadataMap.containsKey(topicName);
    }
}