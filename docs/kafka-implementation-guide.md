# Kafka Clone Implementation Guide

## Overview
This guide provides practical implementation steps for building a Kafka clone that supports the DescribeTopicPartitions API.

## Implementation Steps

### 1. Reading Cluster Metadata

```java
// Read the metadata log file
byte[] data = Files.readAllBytes(Paths.get("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"));

// Parse batches and records
for (Batch batch : parseBatches(data)) {
    for (Record record : batch.getRecords()) {
        if (record.getType() == 2) {  // Topic record
            String topicName = record.getTopicName();
            UUID topicId = record.getTopicId();
            topics.put(topicName, new TopicMetadata(topicName, topicId));
        } else if (record.getType() == 3) {  // Partition record
            UUID topicId = record.getTopicId();
            int partitionId = record.getPartitionId();
            int leaderId = record.getLeaderId();
            int leaderEpoch = record.getLeaderEpoch();
            
            // Find topic by UUID and add partition
            for (TopicMetadata topic : topics.values()) {
                if (topic.getTopicId().equals(topicId)) {
                    topic.addPartition(new PartitionMetadata(partitionId, leaderId, leaderEpoch));
                }
            }
        }
    }
}
```

### 2. Parsing DescribeTopicPartitions Request

```java
// Request format (from hexdump):
// 02 04 62 61 72 00 00 00 00 01 FF 00
// 02 - Tagged field
// 04 - Array length (compact format, 4-1=3 topics)
// 04 - Topic name length (compact format, 4-1=3 bytes)
// 62 61 72 - "bar" in ASCII
// 00 00 00 00 - Partition count (0)
// 01 - Cursor
// FF 00 - End of request

public String parseTopicName(byte[] body) {
    if (body.length < 3) return "";
    
    int index = 1;  // Skip tagged field
    byte arrayLength = body[index++];
    byte topicNameLength = body[index++];
    
    int nameLength = (topicNameLength & 0xFF) - 1;  // -1 for compact string format
    if (nameLength <= 0 || index + nameLength > body.length) return "";
    
    byte[] topicNameBytes = new byte[nameLength];
    System.arraycopy(body, index, topicNameBytes, 0, nameLength);
    return new String(topicNameBytes, StandardCharsets.UTF_8);
}
```

### 3. Building DescribeTopicPartitions Response

```java
public byte[] buildResponse(int correlationId, String topicName) {
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    
    // Response header
    buffer.putInt(correlationId);
    buffer.put((byte) 0);  // Tagged fields
    
    // Response body
    buffer.putInt(0);  // throttle_time_ms
    buffer.put((byte) 2);  // topics_array_length (compact format, 1 topic)
    
    // Topic entry
    TopicMetadata metadata = getTopicMetadata(topicName);
    boolean isKnown = metadata != null;
    
    buffer.putShort(isKnown ? (short) 0 : (short) 3);  // error_code
    
    // Topic name
    byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
    buffer.put((byte) (topicNameBytes.length + 1));  // Length (compact format)
    buffer.put(topicNameBytes);
    
    // Topic ID
    if (isKnown) {
        buffer.putLong(metadata.getTopicId().getMostSignificantBits());
        buffer.putLong(metadata.getTopicId().getLeastSignificantBits());
    } else {
        buffer.put(new byte[16]);  // Zeroed UUID
    }
    
    // Is internal flag
    buffer.put((byte) 0);
    
    // Partitions array
    if (isKnown && !metadata.getPartitions().isEmpty()) {
        buffer.put((byte) 2);  // partitions_array_length (1 partition + 1)
        PartitionMetadata partition = metadata.getPartitions().get(0);
        
        buffer.putShort((short) 0);  // error_code
        buffer.putInt(partition.getPartitionIndex());
        buffer.putInt(partition.getLeaderId());
        buffer.putInt(partition.getLeaderEpoch());
        
        // Empty arrays
        buffer.put((byte) 1);  // replica_nodes
        buffer.put((byte) 1);  // isr_nodes
        buffer.put((byte) 1);  // eligible_leader_replicas
        buffer.put((byte) 1);  // last_known_elr
        buffer.put((byte) 1);  // offline_replicas
        
        buffer.put((byte) 0);  // partition tagged_fields
    } else {
        buffer.put((byte) 1);  // empty partitions array
    }
    
    // Topic authorized operations
    buffer.putInt(0x00000DF8);
    buffer.put((byte) 0);  // topic_tag_buffer
    
    // Cursor (null)
    buffer.put((byte) 0xFF);
    
    // Tagged fields
    buffer.put((byte) 0);
    
    // Create final message with size prefix
    byte[] responseBody = new byte[buffer.position()];
    buffer.flip();
    buffer.get(responseBody);
    
    ByteBuffer finalBuffer = ByteBuffer.allocate(4 + responseBody.length);
    finalBuffer.putInt(responseBody.length);
    finalBuffer.put(responseBody);
    
    return finalBuffer.array();
}
```

## Common Pitfalls and Solutions

| Issue | Solution |
|-------|----------|
| Topic name not parsed correctly | Use direct byte array parsing instead of ByteBuffer |
| Wrong error code in response | Verify topic existence before setting error code |
| Missing fields in response | Follow the protocol specification exactly |
| Incorrect cursor format | Use 0xFF for null cursor |
| Partition fields in wrong order | Error code should come before partition index |
| Empty arrays not formatted correctly | Use compact format (length byte = 1) |
| Response size incorrect | Include 4-byte size prefix before the actual response |

## Testing

1. Verify the response structure matches the protocol specification
2. Check that correlation IDs match between request and response
3. Confirm error codes are correct (0 for existing topics, 3 for unknown)
4. Ensure topic UUIDs match those in the metadata
5. Validate partition information is correctly included
