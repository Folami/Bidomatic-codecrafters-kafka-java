// package codecrafters_kafka_java; // Uncomment if using package

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Metadata class for parsing __cluster_metadata log, mimicking metadata.py.
 */
public class Metadata {
    private final Map<byte[], TopicInfo> topics;
    private final Map<byte[], PartitionInfo> partitions;
    private int batches;

    public Metadata(byte[] file) {
        this.topics = new HashMap<>();
        this.partitions = new HashMap<>();
        this.batches = 0;
        parseLogFile(file);
        linkPartitionsToTopics();
    }

    private void linkPartitionsToTopics() {
        for (Map.Entry<byte[], PartitionInfo> partitionEntry : partitions.entrySet()) {
            UUID topicUuid = partitionEntry.getValue().topicUuid;
            for (Map.Entry<byte[], TopicInfo> topicEntry : topics.entrySet()) {
                if (topicEntry.getValue().uuid.equals(topicUuid)) {
                    topicEntry.getValue().partitions.add(partitionEntry.getKey());
                    break;
                }
            }
        }
    }

    private void parseLogFile(byte[] file) {
        List<byte[]> batches = separateBatches(file);
        for (byte[] batch : batches) {
            parseBatch(batch);
        }
    }

    private List<byte[]> separateBatches(byte[] file) {
        ByteParser parser = new ByteParser(file);
        List<byte[]> batches = new ArrayList<>();
        while (!parser.eof()) {
            byte[] offset = parser.consume(8);
            System.err.println("offset " + ByteBuffer.wrap(offset).order(ByteOrder.BIG_ENDIAN).getLong());
            this.batches++;
            int batchLength = parser.consumeInt();
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
        parser.consume(4); // offset
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
            byte[] record = parser.consume(length);
            records.add(record);
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
            parser.consume(1); // key
        }
        int valueLength = parser.consumeVarInt(true);
        byte[] value = parser.consume(valueLength);
        parseValue(value);
    }

    private void parseValue(byte[] value) {
        ByteParser parser = new ByteParser(value);
        parser.consume(1); // frame_version
        int type = parser.consume(1)[0] & 0xFF;
        switch (type) {
            case 2:
                parseTopic(parser);
                break;
            case 3:
                parsePartition(parser);
                break;
            default:
                System.err.println("Unknown value type: " + type);
        }
    }

    private void parseTopic(ByteParser parser) {
        parser.consume(1); // version
        int lengthOfName = parser.consumeVarInt(false) - 1;
        byte[] topicName = parser.consume(lengthOfName);
        byte[] rawUuid = parser.consume(16);
        UUID uuid = new UUID(
                ByteBuffer.wrap(rawUuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong(),
                ByteBuffer.wrap(rawUuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong()
        );
        topics.put(topicName, new TopicInfo(uuid, new ArrayList<>()));
    }

    private void parsePartition(ByteParser parser) {
        parser.consume(1); // version
        byte[] partitionId = parser.consume(4);
        byte[] rawUuid = parser.consume(16);
        UUID topicUuid = new UUID(
                ByteBuffer.wrap(rawUuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong(),
                ByteBuffer.wrap(rawUuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong()
        );
        int lengthOfReplicaArray = parser.consumeVarInt(false) - 1;
        digestArray(parser, lengthOfReplicaArray, 4);
        int lengthOfInSync = parser.consumeVarInt(false) - 1;
        List<Integer> inSync = digestArray(parser, lengthOfInSync, 4);
        digestArray(parser, parser.consumeVarInt(false) - 1, 4); // removing
        digestArray(parser, parser.consumeVarInt(false) - 1, 4); // adding
        int leader = parser.consumeInt();
        int epoch = parser.consumeInt();
        parser.consume(4); // partition_epoch
        digestArray(parser, parser.consumeVarInt(false) - 1, 4); // directories
        partitions.put(partitionId, new PartitionInfo(partitionId, leader, epoch, topicUuid));
    }

    private List<Integer> digestArray(ByteParser parser, int length, int sizePerItem) {
        List<Integer> ret = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            byte[] item = parser.consume(sizePerItem);
            ret.add(ByteBuffer.wrap(item).order(ByteOrder.BIG_ENDIAN).getInt());
        }
        return ret;
    }

    public Map<byte[], TopicInfo> getTopics() {
        return topics;
    }

    public Map<byte[], PartitionInfo> getPartitions() {
        return partitions;
    }

    public static class TopicInfo {
        public final UUID uuid;
        public final List<byte[]> partitions;

        public TopicInfo(UUID uuid, List<byte[]> partitions) {
            this.uuid = uuid;
            this.partitions = partitions;
        }
    }

    public static class PartitionInfo {
        public final byte[] id;
        public final int leader;
        public final int leaderEpoch;
        public final UUID topicUuid;

        public PartitionInfo(byte[] id, int leader, int leaderEpoch, UUID topicUuid) {
            this.id = id;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.topicUuid = topicUuid;
        }
    }
}