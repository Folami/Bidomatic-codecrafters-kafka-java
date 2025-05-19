import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Metadata class for parsing Kafka cluster metadata.
 * This class mimics the functionality of the Python Metadata class.
 */
public class Metadata {
    private static final UUID BASE_UUID = new UUID(0L, 0L);

    private ByteParser parser;
    private int batches = 0;
    private Map<String, Map<String, Object>> topics = new HashMap<>();
    private Map<byte[], Map<String, Object>> partitions = new HashMap<>();

    /**
     * Constructor for Metadata.
     * @param file The byte array containing the metadata log file.
     */
    public Metadata(byte[] file) {
        this.parser = new ByteParser(file);
        this.batches = 0;

        // Initialize topics with default values
        // In Python: self.topics = defaultdict(lambda: {"uuid": BASE_UUID, "partitions": []})

        parseLogFile();

        // Link partitions to topics
        for (byte[] id : partitions.keySet()) {
            @SuppressWarnings("unchecked")
            List<UUID> topicUuids = (List<UUID>) partitions.get(id).get("topics");

            for (UUID uuid : topicUuids) {
                for (String topic : topics.keySet()) {
                    if (uuid.equals(topics.get(topic).get("uuid"))) {
                        @SuppressWarnings("unchecked")
                        List<byte[]> topicPartitions = (List<byte[]>) topics.get(topic).get("partitions");
                        topicPartitions.add(id);
                    }
                }
            }
        }
    }

    /**
     * Parse the log file.
     */
    private void parseLogFile() {
        List<byte[]> batches = separateBatches();
        for (byte[] batch : batches) {
            parseBatch(batch);
        }
    }

    /**
     * Separate the log file into batches.
     * @return A list of batch byte arrays.
     */
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

    /**
     * Parse a batch.
     * @param batch The batch byte array.
     */
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

    /**
     * Separate records from a batch.
     * @param parser The parser for the batch.
     * @return A list of record byte arrays.
     */
    private List<byte[]> separateRecords(ByteParser parser) {
        List<byte[]> records = new ArrayList<>();
        while (!parser.eof()) {
            int length = parser.consumeVarInt(true);
            records.add(parser.consume(length));
        }
        return records;
    }

    /**
     * Parse a record.
     * @param batch The record byte array.
     */
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

    /**
     * Parse a value.
     * @param value The value byte array.
     */
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

    /**
     * Parse a topic.
     * @param parser The parser for the topic.
     */
    private void parseTopic(ByteParser parser) {
        parser.consume(1); // version
        int lengthOfName = parser.consumeVarInt(false) - 1;
        byte[] topicNameBytes = parser.consume(lengthOfName);
        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
        byte[] rawUuid = parser.consume(16);

        UUID uuid = bytesToUuid(rawUuid);

        Map<String, Object> topicInfo = new HashMap<>();
        topicInfo.put("uuid", uuid);
        topicInfo.put("partitions", new ArrayList<byte[]>());

        topics.put(topicName, topicInfo);
    }

    /**
     * Parse a partition.
     * @param parser The parser for the partition.
     */
    private void parsePartition(ByteParser parser) {
        parser.consume(1); // version
        byte[] partitionId = parser.consume(4);
        byte[] rawUuid = parser.consume(16);
        UUID uuid = bytesToUuid(rawUuid);

        int lengthOfReplicaArray = parser.consumeVarInt(false) - 1;
        List<byte[]> replicas = digestArray(parser, lengthOfReplicaArray, 4);

        int lengthOfInSyncArray = parser.consumeVarInt(false) - 1;
        List<byte[]> inSync = digestArray(parser, lengthOfInSyncArray, 4);

        int lengthOfRemovingArray = parser.consumeVarInt(false) - 1;
        List<byte[]> removing = digestArray(parser, lengthOfRemovingArray, 4);

        int lengthOfAddingArray = parser.consumeVarInt(false) - 1;
        List<byte[]> adding = digestArray(parser, lengthOfAddingArray, 4);

        byte[] leader = parser.consume(4);
        byte[] epoch = parser.consume(4);
        parser.consume(4); // partition_epoch

        int lengthOfDirectoriesArray = parser.consumeVarInt(false) - 1;
        List<byte[]> directories = digestArray(parser, lengthOfDirectoriesArray, 4);

        if (partitions.containsKey(partitionId)) {
            @SuppressWarnings("unchecked")
            List<UUID> topicUuids = (List<UUID>) partitions.get(partitionId).get("topics");
            topicUuids.add(uuid);
        } else {
            Map<String, Object> partitionInfo = new HashMap<>();
            List<UUID> topicUuids = new ArrayList<>();
            topicUuids.add(uuid);

            partitionInfo.put("topics", topicUuids);
            partitionInfo.put("id", partitionId);
            partitionInfo.put("leader", leader);
            partitionInfo.put("leader_epoch", epoch);

            partitions.put(partitionId, partitionInfo);
        }
    }

    /**
     * Process an array of items.
     * @param parser The parser for the array.
     * @param length The number of items in the array.
     * @param sizePerItem The size of each item.
     * @return A list of item byte arrays.
     */
    private List<byte[]> digestArray(ByteParser parser, int length, int sizePerItem) {
        List<byte[]> ret = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            ret.add(parser.consume(sizePerItem));
        }
        return ret;
    }

    /**
     * Convert bytes to a UUID.
     * @param bytes The bytes to convert.
     * @return The UUID.
     */
    private UUID bytesToUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low);
    }

    /**
     * Get the topics.
     * @return The topics.
     */
    public Map<String, Map<String, Object>> getTopics() {
        return topics;
    }

    /**
     * Get the partitions.
     * @return The partitions.
     */
    public Map<byte[], Map<String, Object>> getPartitions() {
        return partitions;
    }
}
