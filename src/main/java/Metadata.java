import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Metadata class for parsing Kafka cluster metadata, similar to the Python implementation.
 */
public class Metadata {
    private static final UUID BASE_UUID = new UUID(0L, 0L);
    private ByteParser parser;
    private int batches = 0;
    private Map<byte[], TopicInfo> topics = new HashMap<>();
    private Map<byte[], PartitionInfo> partitions = new HashMap<>();

    public static class TopicInfo {
        public UUID uuid;
        public List<byte[]> partitions;

        public TopicInfo(UUID uuid) {
            this.uuid = uuid;
            this.partitions = new ArrayList<>();
        }
    }

    public static class PartitionInfo {
        public List<UUID> topics;
        public byte[] id;
        public byte[] leader;
        public byte[] leaderEpoch;

        public PartitionInfo(UUID topicUuid, byte[] id, byte[] leader, byte[] leaderEpoch) {
            this.topics = new ArrayList<>();
            this.topics.add(topicUuid);
            this.id = id;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
        }
    }

    public Metadata(byte[] file) {
        parser = new ByteParser(file);
        parseLogFile();
        
        // Link partitions to topics
        for (Map.Entry<byte[], PartitionInfo> entry : partitions.entrySet()) {
            byte[] id = entry.getKey();
            PartitionInfo partition = entry.getValue();
            
            for (UUID topicUuid : partition.topics) {
                for (Map.Entry<byte[], TopicInfo> topicEntry : topics.entrySet()) {
                    if (topicEntry.getValue().uuid.equals(topicUuid)) {
                        topicEntry.getValue().partitions.add(id);
                    }
                }
            }
        }
    }

    private void parseLogFile() {
        List<byte[]> batches = separateBatches();
        for (byte[] batch : batches) {
            parseBatch(batch);
        }
    }

    private List<byte[]> separateBatches() {
        List<byte[]> batches = new ArrayList<>();
        while (!parser.isFinished()) {
            byte[] offset = parser.consume(8);
            System.out.println("offset " + ByteBuffer.wrap(offset).getLong());
            this.batches++;
            
            byte[] batchLengthBytes = parser.consume(4);
            int batchLength = ByteBuffer.wrap(batchLengthBytes).getInt();
            
            batches.add(parser.consume(batchLength));
        }
        return batches;
    }

    private void parseBatch(byte[] batch) {
        ByteParser parser = new ByteParser(batch);
        
        parser.consume(4); // ple
        parser.consume(1); // mb
        parser.consume(4); // crc
        parser.consume(2); // type
        byte[] offsetBytes = parser.consume(4);
        int offset = ByteBuffer.wrap(offsetBytes).getInt();
        int length = offset + 1;
        
        parser.consume(8); // created_at
        parser.consume(8); // updated_at
        parser.consume(8); // p_id
        parser.consume(2); // p_epoch
        parser.consume(4); // base_sequence
        parser.consume(4); // record_count
        
        List<byte[]> records = separateRecords(parser);
        for (byte[] record : records) {
            parseRecord(record);
        }
    }

    private List<byte[]> separateRecords(ByteParser parser) {
        List<byte[]> records = new ArrayList<>();
        while (!parser.eof()) {
            int length = parser.consumeVarInt(true);
            records.add(parser.consume(length));
        }
        return records;
    }

    private void parseRecord(byte[] batch) {
        ByteParser parser = new ByteParser(batch);
        
        parser.consume(1); // attribute
        parser.consumeVarInt(true); // timestamp_delta
        parser.consumeVarInt(true); // offset_delta
        
        int keyLength = parser.consumeVarInt(true);
        if (keyLength != -1) {
            parser.read(1);
        }
        
        int valueLength = parser.consumeVarInt(true);
        byte[] value = parser.consume(valueLength);
        
        parseValue(value);
    }

    private void parseValue(byte[] value) {
        ByteParser parser = new ByteParser(value);
        
        parser.consume(1); // frame_version
        byte[] typeBytes = parser.consume(1);
        int type = typeBytes[0] & 0xFF;
        
        if (type == 2) {
            parseTopic(parser);
        } else if (type == 3) {
            parsePartition(parser);
        }
    }

    private void parseTopic(ByteParser parser) {
        parser.consume(1); // version
        int lengthOfName = parser.consumeVarInt(false) - 1;
        byte[] topicName = parser.consume(lengthOfName);
        byte[] rawUuid = parser.consume(16);
        
        UUID uuid = bytesToUuid(rawUuid);
        topics.put(topicName, new TopicInfo(uuid));
    }

    private void parsePartition(ByteParser parser) {
        parser.consume(1); // version
        byte[] partitionId = parser.consume(4);
        byte[] rawUuid = parser.consume(16);
        UUID uuid = bytesToUuid(rawUuid);
        
        int lengthOfReplicaArray = parser.consumeVarInt(false) - 1;
        digestArray(parser, lengthOfReplicaArray, 4); // replicas
        
        int lengthOfInSyncArray = parser.consumeVarInt(false) - 1;
        digestArray(parser, lengthOfInSyncArray, 4); // in_sync
        
        int lengthOfRemovingArray = parser.consumeVarInt(false) - 1;
        digestArray(parser, lengthOfRemovingArray, 4); // removing
        
        int lengthOfAddingArray = parser.consumeVarInt(false) - 1;
        digestArray(parser, lengthOfAddingArray, 4); // adding
        
        byte[] leader = parser.consume(4);
        byte[] epoch = parser.consume(4);
        parser.consume(4); // partition_epoch
        
        int lengthOfDirectoriesArray = parser.consumeVarInt(false) - 1;
        digestArray(parser, lengthOfDirectoriesArray, 4); // directories
        
        if (partitions.containsKey(partitionId)) {
            partitions.get(partitionId).topics.add(uuid);
        } else {
            partitions.put(partitionId, new PartitionInfo(uuid, partitionId, leader, epoch));
        }
    }

    private List<byte[]> digestArray(ByteParser parser, int length, int sizePerItem) {
        List<byte[]> ret = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            ret.add(parser.consume(sizePerItem));
        }
        return ret;
    }

    private UUID bytesToUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }

    public Map<byte[], TopicInfo> getTopics() {
        return topics;
    }

    public Map<byte[], PartitionInfo> getPartitions() {
        return partitions;
    }
}
