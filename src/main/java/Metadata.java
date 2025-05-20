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
    private int batches;
    private Map<String, Map<String, Object>> topics;
    private Map<byte[], Map<String, Object>> partitions;

    /**
     * Constructor for Metadata.
     * @param file The byte array containing the metadata log file.
     */
    public Metadata(byte[] file) {
        this.parser = new ByteParser(file);
        this.batches = 0;
        this.topics = new HashMap<>();
        this.partitions = new HashMap<>();

        parseLogFile();

        // Link partitions to topics
        for (byte[] id : partitions.keySet()) {
            @SuppressWarnings("unchecked")
            List<UUID> topicUUIDs = (List<UUID>) partitions.get(id).get("topics");

            for (UUID uuid : topicUUIDs) {
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
        ByteParser parser = this.parser;
        List<byte[]> batches = new ArrayList<>();
        while (!parser.eof()) {
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

        int key_length = parser.consumeVarInt(true);
        if (key_length != -1) {
            parser.read(1);
        }

        int value_length = parser.consumeVarInt(true);
        byte[] value = parser.consume(value_length);

        parseValue(value);
    }

    /**
     * Parse a value.
     * @param value The value byte array.
     */
    private void parseValue(byte[] value) {
        ByteParser parser = new ByteParser(value);

        parser.consume(1); // frame_version
        byte[] type_bytes = parser.consume(1);
        int type = type_bytes[0] & 0xFF;

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
        int length_of_name = parser.consumeVarInt(false) - 1;
        byte[] topic_name_bytes = parser.consume(length_of_name);
        String topic_name = new String(topic_name_bytes, StandardCharsets.UTF_8);
        byte[] raw_uuid = parser.consume(16);

        UUID uuid = new UUID(
            ByteBuffer.wrap(raw_uuid, 0, 8).getLong(),
            ByteBuffer.wrap(raw_uuid, 8, 8).getLong()
        );

        Map<String, Object> topic_info = new HashMap<>();
        topic_info.put("uuid", uuid);
        topic_info.put("partitions", new ArrayList<byte[]>());

        topics.put(topic_name, topic_info);
    }

    /**
     * Parse a partition.
     * @param parser The parser for the partition.
     */
    private void parsePartition(ByteParser parser) {
        parser.consume(1); // version
        byte[] partition_id = parser.consume(4);
        byte[] raw_uuid = parser.consume(16);
        UUID uuid = new UUID(
            ByteBuffer.wrap(raw_uuid, 0, 8).getLong(),
            ByteBuffer.wrap(raw_uuid, 8, 8).getLong()
        );

        int length_of_replica_array = parser.consumeVarInt(false) - 1;
        digestArray(parser, length_of_replica_array, 4);

        int length_of_in_sync_array = parser.consumeVarInt(false) - 1;
        digestArray(parser, length_of_in_sync_array, 4);

        int length_of_removing_array = parser.consumeVarInt(false) - 1;
        digestArray(parser, length_of_removing_array, 4);

        int length_of_adding_array = parser.consumeVarInt(false) - 1;
        digestArray(parser, length_of_adding_array, 4);

        byte[] leader = parser.consume(4);
        byte[] epoch = parser.consume(4);
        parser.consume(4); // partition_epoch

        int length_of_directories_array = parser.consumeVarInt(false) - 1;
        digestArray(parser, length_of_directories_array, 4);

        if (partitions.containsKey(partition_id)) {
            @SuppressWarnings("unchecked")
            List<UUID> topicUUIDs = (List<UUID>) partitions.get(partition_id).get("topics");
            topicUUIDs.add(uuid);
        } else {
            Map<String, Object> partition_info = new HashMap<>();
            List<UUID> topicUUIDs = new ArrayList<>();
            topicUUIDs.add(uuid);

            partition_info.put("topics", topicUUIDs);
            partition_info.put("id", partition_id);
            partition_info.put("leader", leader);
            partition_info.put("leader_epoch", epoch);

            partitions.put(partition_id, partition_info);
        }
    }

    /**
     * Process an array of items.
     * @param parser The parser for the array.
     * @param length The number of items in the array.
     * @param size_per_item The size of each item.
     * @return A list of item byte arrays.
     */
    private List<byte[]> digestArray(ByteParser parser, int length, int size_per_item) {
        List<byte[]> ret = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            ret.add(parser.consume(size_per_item));
        }
        return ret;
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