import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Main class for the Kafka clone implementation.
 * This class mimics the functionality of the Python main.py file.
 */
public class Main {
    private static final String METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    // Constants
    private static final byte[] TAG_BUFFER = new byte[]{0};
    private static final byte[] DEFAULT_THROTTLE_TIME = new byte[]{0, 0, 0, 0};
    private static final Map<String, byte[]> ERRORS = new HashMap<>();
    static {
        ERRORS.put("ok", new byte[]{0, 0});
        ERRORS.put("error", new byte[]{0, 35});
    }

    /**
     * Base class for Kafka message handling.
     */
    static abstract class BaseKafka {
        /**
         * Create a message with size prefix.
         * @param message The message bytes.
         * @return The message with size prefix.
         */
        protected byte[] createMessage(byte[] message) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.putInt(message.length);
            buffer.put(message);
            return buffer.array();
        }

        /**
         * Remove tag buffer from the beginning of a buffer.
         * @param buffer The buffer.
         * @return The buffer without the tag buffer.
         */
        protected byte[] removeTagBuffer(byte[] buffer) {
            byte[] result = new byte[buffer.length - 1];
            System.arraycopy(buffer, 1, result, 0, buffer.length - 1);
            return result;
        }

        /**
         * Parse a string from a buffer.
         * @param buffer The buffer.
         * @return The string and the remaining buffer.
         */
        protected StringParseResult parseString(byte[] buffer) {
            int length = ByteBuffer.wrap(buffer, 0, 2).order(ByteOrder.BIG_ENDIAN).getShort() & 0xFFFF;
            byte[] stringBytes = new byte[length];
            System.arraycopy(buffer, 2, stringBytes, 0, length);
            String string = new String(stringBytes, StandardCharsets.UTF_8);
            byte[] remaining = new byte[buffer.length - 2 - length];
            System.arraycopy(buffer, 2 + length, remaining, 0, buffer.length - 2 - length);
            return new StringParseResult(string, remaining);
        }

        /**
         * Result of parsing a string.
         */
        static class StringParseResult {
            public final String value;
            public final byte[] remaining;

            public StringParseResult(String value, byte[] remaining) {
                this.value = value;
                this.remaining = remaining;
            }
        }

        /**
         * Parse an array from a buffer.
         * @param buffer The buffer.
         * @param consumer The consumer to apply to each item.
         * @return The remaining buffer.
         */
        protected byte[] parseArray(byte[] buffer, ArrayItemConsumer consumer) {
            if (buffer == null || buffer.length < 1) { // Need at least 1 byte for array length
                System.err.println("BaseKafka.parseArray: Input buffer is null or too short for array length.");
                return new byte[0];
            }
            int arrayCompactLength = buffer[0] & 0xFF;
            int numberOfElements = arrayCompactLength - 1;
            int currentOffset = 1; // Start after the array compact length byte
            if (numberOfElements < 0) {
                System.err.println("BaseKafka.parseArray: Invalid number of elements (" + numberOfElements + ") from compact_length_byte " + arrayCompactLength);
                // Return buffer after consuming the invalid length byte
                return copyOfRange(buffer, currentOffset, buffer.length);
            }
            for (int i = 0; i < numberOfElements; i++) {
                // Check if there's enough space for the item's compact length byte
                if (currentOffset >= buffer.length) {
                    System.err.println("BaseKafka.parseArray: Buffer too short for item " + i + "'s compact length byte.");
                    break;
                }
                int itemCompactLength = buffer[currentOffset] & 0xFF; // This is the L byte value (actual_length + 1)
                int itemActualStringLength = itemCompactLength - 1;
                if (itemActualStringLength < 0) {
                    System.err.println("BaseKafka.parseArray: Invalid actual string length (" + itemActualStringLength + ") for item " + i + " from compact_length " + itemCompactLength);
                    // Advance offset by itemCompactLength + 1 to skip this malformed item and the extra byte
                    currentOffset += (itemCompactLength + 1); // This might be problematic if itemCompactLength is large and invalid
                    continue; // Or break, depending on desired error handling
                }
                // Check if there's enough space for the item's content (string + compact_length_byte + 1 extra byte)
                if (currentOffset + itemCompactLength + 1 > buffer.length) {
                    System.err.println("BaseKafka.parseArray: Buffer too short for item " + i + " content and extra byte. Needed: " + (itemCompactLength + 1) + " from offset " + currentOffset + ", available: " + (buffer.length - currentOffset));
                    break;
                }
                // Extract the string: starts 1 byte after itemCompactLength byte, for itemActualStringLength bytes
                byte[] itemData = copyOfRange(buffer, currentOffset + 1, currentOffset + 1 + itemActualStringLength);
                consumer.consume(itemData);
                // Advance offset by itemCompactLength (which includes its own byte and string bytes) + 1 (for the extra byte)
                currentOffset += (itemCompactLength + 1);
            }
            return copyOfRange(buffer, currentOffset, buffer.length);
        }
    }

    /**
     * Interface for consuming array items.
     */
    interface ArrayItemConsumer {
        void consume(byte[] item);
    }

    /**
     * Kafka header class.
     */
    static class KafkaHeader extends BaseKafka {
        public final byte[] length;
        public final byte[] key;
        public final int keyInt;
        public final byte[] version;
        public final int versionInt;
        public final byte[] id;
        public final String client;
        public final byte[] body;

        public KafkaHeader(byte[] data) {
            this.length = new byte[4];
            System.arraycopy(data, 0, this.length, 0, 4);
            this.key = new byte[2];
            System.arraycopy(data, 4, this.key, 0, 2);
            this.keyInt = ByteBuffer.wrap(this.key).order(ByteOrder.BIG_ENDIAN).getShort() & 0xFFFF;
            this.version = new byte[2];
            System.arraycopy(data, 6, this.version, 0, 2);
            this.versionInt = ByteBuffer.wrap(this.version).order(ByteOrder.BIG_ENDIAN).getShort() & 0xFFFF;
            this.id = new byte[4];
            System.arraycopy(data, 8, this.id, 0, 4);
            byte[] clientBuffer = new byte[data.length - 12];
            System.arraycopy(data, 12, clientBuffer, 0, data.length - 12);
            StringParseResult clientResult = parseString(clientBuffer);
            this.client = clientResult.value;
            this.body = removeTagBuffer(clientResult.remaining);
        }
    }

    /**
     * API request class.
     */
    static class ApiRequest extends BaseKafka {
        private final int versionInt;
        private final byte[] id;
        public final byte[] message;

        public ApiRequest(int versionInt, byte[] id) {
            this.versionInt = versionInt;
            this.id = id;
            this.message = createMessage(constructMessage());
        }

        private byte[] constructMessage() {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            try {
                // Add correlation ID
                body.write(id);
                // Add error code
                body.write(errorHandler());
                // Add API keys array
                body.write(3); // Array length (compact format, 3-1=2 elements)
                // ApiVersions entry (key 18)
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 18).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 4).array());
                body.write(0); // Tagged fields
                // DescribeTopicPartitions entry (key 75)
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 75).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                body.write(0); // Tagged fields
                // Add throttle time and tag buffer
                body.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0).array());
                body.write(0); // Tag buffer
                return body.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }

        private byte[] errorHandler() {
            if (0 <= versionInt && versionInt <= 4) {
                return ERRORS.get("ok");
            } else {
                return ERRORS.get("error");
            }
        }
    }

    /**
     * DescribeTopicPartitions request class.
     */
    static class DescribeTopicPartitionsRequest extends BaseKafka {
        private final byte[] id;
        private final byte[] body;
        private final List<String> topics = new ArrayList<>();
        private final byte[] cursor;
        private final Map<String, Map<String, Object>> availableTopics;
        private final Map<byte[], Map<String, Object>> partitions;
        public final byte[] message;

        public DescribeTopicPartitionsRequest(byte[] id, byte[] body, Metadata metadata) {
            this.id = id;
            this.body = body;

            // Parse topics array
            byte[] buffer = parseArray(body, this::parseTopics);

            // Extract cursor
            this.cursor = new byte[1];
            System.arraycopy(buffer, 0, this.cursor, 0, 1);

            // Remove tag buffer
            buffer = removeTagBuffer(buffer);

            // Get metadata
            this.availableTopics = metadata.getTopics();
            this.partitions = metadata.getPartitions();

            // Create message
            this.message = createMessage(constructMessage());
        }

        private void parseTopics(byte[] itemBuffer) {
            topics.add(new String(itemBuffer, StandardCharsets.UTF_8));
        }

        private byte[] createTopicItem(byte[] topic) {
            String topicStr = new String(topic, StandardCharsets.UTF_8);
            boolean available = availableTopics.containsKey(topicStr);
            ByteArrayOutputStream topicBuffer = new ByteArrayOutputStream();
            try {
                // Error code
                if (available) {
                    topicBuffer.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                } else {
                    topicBuffer.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 3).array());
                }
                // Topic name
                topicBuffer.write((byte) (topic.length + 1)); // Compact string length
                topicBuffer.write(topic);
                // Topic ID (UUID)
                if (available) {
                    Map<String, Object> topicInfo = availableTopics.get(topicStr);
                    UUID uuid = (UUID) topicInfo.get("uuid");
                    ByteBuffer uuidBuffer = ByteBuffer.allocate(16);
                    uuidBuffer.putLong(uuid.getMostSignificantBits());
                    uuidBuffer.putLong(uuid.getLeastSignificantBits());
                    topicBuffer.write(uuidBuffer.array());
                } else {
                    topicBuffer.write(new byte[16]); // Zeroed UUID
                }
                // Is internal flag
                topicBuffer.write(0);
                // Partitions array
                if (available) {
                    Map<String, Object> topicInfo = availableTopics.get(topicStr);
                    @SuppressWarnings("unchecked")
                    List<byte[]> topicPartitions = (List<byte[]>) topicInfo.get("partitions");
                    if (topicPartitions != null && !topicPartitions.isEmpty()) {
                        topicBuffer.write((byte) (topicPartitions.size() + 1)); // Compact array length
                        for (byte[] partitionId : topicPartitions) {
                            topicBuffer.write(addPartition(partitions.get(partitionId)));
                        }
                    } else {
                        topicBuffer.write((byte) 1); // Empty array
                    }
                } else {
                    topicBuffer.write((byte) 1); // Empty array
                }
                // Topic authorized operations
                topicBuffer.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0x00000DF8).array());
                // Tag buffer
                topicBuffer.write((byte) 0);
                return topicBuffer.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }

        private byte[] addPartition(Map<String, Object> partition) {
            ByteArrayOutputStream ret = new ByteArrayOutputStream();
            try {
                // Error code
                ret.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                // Partition index
                ret.write((byte[]) partition.get("id"));
                // Leader
                ret.write((byte[]) partition.get("leader"));
                // Leader epoch
                ret.write((byte[]) partition.get("leader_epoch"));
                // Empty arrays
                ret.write((byte) 1); // replica_nodes
                ret.write((byte) 1); // isr_nodes
                ret.write((byte) 1); // eligible_leader_replicas (mimicking python output which includes these for v0)
                ret.write((byte) 1); // last_known_elr (mimicking python output)
                ret.write((byte) 1); // offline_replicas
                // Tagged fields
                ret.write((byte) 0);
                return ret.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }

        private byte[] constructMessage() {
            ByteArrayOutputStream message = new ByteArrayOutputStream();
            try {
                // Header
                message.write(id);
                message.write(TAG_BUFFER);
                // Response body
                message.write(DEFAULT_THROTTLE_TIME);
                // Topics array
                message.write((byte) (topics.size() + 1)); // Compact array length
                // Add all topic information
                for (String topic : topics) {
                    message.write(createTopicItem(topic.getBytes(StandardCharsets.UTF_8)));
                }
                // Add cursor (null cursor)
                message.write((byte) 0xFF);
                // Tagged fields
                message.write(TAG_BUFFER);
                return message.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }
    }

    private static byte[] copyOfRange(byte[] original, int from, int to) {
        if (from > to || from < 0 || to > original.length) {
            // Consider logging an error or throwing IllegalArgumentException if from/to are invalid
            return new byte[0]; // Return empty if range is invalid or empty
        }
        int newLength = to - from;
        byte[] copy = new byte[newLength];
        System.arraycopy(original, from, copy, 0, Math.min(original.length - from, newLength));
        return copy;
    }

    /**
     * Topic request class.
     */
    static class TopicRequest extends BaseKafka {
        private final byte[] id;
        private final byte[] body;
        private final List<String> topics = new ArrayList<>();
        private final byte[] limit;
        private final byte[] cursor;
        private final Map<String, Map<String, Object>> availableTopics;
        private final Map<byte[], Map<String, Object>> partitions;
        public final byte[] message;

        public TopicRequest(byte[] id, byte[] body, Metadata metadata) {
            this.id = id;
            this.body = body;
            // Parse topics array
            byte[] buffer = parseArray(body, this::parseTopics);
            // Extract limit and cursor
            this.limit = new byte[4];
            System.arraycopy(buffer, 0, this.limit, 0, 4);
            this.cursor = new byte[1];
            System.arraycopy(buffer, 4, this.cursor, 0, 1);
            // Get metadata
            this.availableTopics = metadata.getTopics();
            this.partitions = metadata.getPartitions(); // Initialize partitions here
            // Create message
            this.message = createMessage(constructMessage());
        }

        private void parseTopics(byte[] itemBuffer) {
            String topic = new String(itemBuffer, StandardCharsets.UTF_8);
            topics.add(topic);
        }

        private byte[] createTopicItem(byte[] topic) {
            String topicStr = new String(topic, StandardCharsets.UTF_8);
            boolean available = availableTopics.containsKey(topicStr);
            ByteArrayOutputStream topicBuffer = new ByteArrayOutputStream();
            try {
                // Error code
                if (available) {
                    topicBuffer.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                } else {
                    topicBuffer.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 3).array());
                }
                // Topic name
                topicBuffer.write(topic.length + 1); // Compact string length
                topicBuffer.write(topic);
                // Topic ID (UUID)
                if (available) {
                    Map<String, Object> topicInfo = availableTopics.get(topicStr);
                    UUID uuid = (UUID) topicInfo.get("uuid");
                    ByteBuffer uuidBuffer = ByteBuffer.allocate(16);
                    uuidBuffer.putLong(uuid.getMostSignificantBits());
                    uuidBuffer.putLong(uuid.getLeastSignificantBits());
                    topicBuffer.write(uuidBuffer.array());
                } else {
                    topicBuffer.write(new byte[16]); // Zeroed UUID
                }
                // Is internal flag
                topicBuffer.write(0);
                // Partitions array
                if (available) {
                    Map<String, Object> topicInfo = availableTopics.get(topicStr);
                    @SuppressWarnings("unchecked")
                    List<byte[]> topicPartitions = (List<byte[]>) topicInfo.get("partitions");
                    if (topicPartitions != null && !topicPartitions.isEmpty()) {
                        topicBuffer.write(topicPartitions.size() + 1); // Compact array length
                        for (byte[] partitionId : topicPartitions) {
                            topicBuffer.write(addPartition(partitions.get(partitionId)));
                        }
                    } else {
                        topicBuffer.write(1); // Empty array
                    }
                } else {
                    topicBuffer.write(1); // Empty array
                }
                // Topic authorized operations
                topicBuffer.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0x00000DF8).array());
                // Tag buffer
                topicBuffer.write(0);
                return topicBuffer.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }

        private byte[] addPartition(Map<String, Object> partition) {
            ByteArrayOutputStream ret = new ByteArrayOutputStream();
            try {
                // Error code
                ret.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                // Partition index
                byte[] id = (byte[]) partition.get("id");
                ret.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(ByteBuffer.wrap(id).getInt()).array());
                // Leader
                byte[] leader = (byte[]) partition.get("leader");
                ret.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(ByteBuffer.wrap(leader).getInt()).array());
                // Leader epoch
                byte[] leaderEpoch = (byte[]) partition.get("leader_epoch");
                ret.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(ByteBuffer.wrap(leaderEpoch).getInt()).array());
                // Empty arrays
                ret.write(0);
                ret.write(0);
                ret.write(0);
                ret.write(0);
                ret.write(0);
                ret.write(0);
                return ret.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }

        private byte[] constructMessage() {
            ByteArrayOutputStream message = new ByteArrayOutputStream();
            try {
                // Header
                message.write(id);
                message.write(TAG_BUFFER);
                // Add throttle time
                message.write(DEFAULT_THROTTLE_TIME);
                // Topics array
                message.write(topics.size() + 1); // Compact array length
                // Add all topic information
                for (String topic : topics) {
                    message.write(createTopicItem(topic.getBytes(StandardCharsets.UTF_8)));
                }
                // Add cursor
                message.write(0xFF);
                // Tagged fields
                message.write(0);
                return message.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }
    }

    /**
     * Handler for client connections.
     */
    static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final Metadata metadata;

        public ClientHandler(Socket clientSocket, Metadata metadata) {
            this.clientSocket = clientSocket;
            this.metadata = metadata;
        }

        @Override
        public void run() {
            try (InputStream in = clientSocket.getInputStream();
                 OutputStream out = clientSocket.getOutputStream()) {
                byte[] buffer = new byte[1024];
                while (in.read(buffer) != -1) {
                    // Parse the request
                    KafkaHeader header = new KafkaHeader(buffer);
                    // Process based on API key
                    byte[] message;
                    if (header.keyInt == 18) { // ApiVersions
                        ApiRequest request = new ApiRequest(header.versionInt, header.id);
                        message = request.message;
                    } else if (header.keyInt == 75) { // DescribeTopicPartitions
                        DescribeTopicPartitionsRequest request = new DescribeTopicPartitionsRequest(header.id, header.body, metadata);
                        message = request.message;
                    } else { // Default to TopicRequest
                        TopicRequest request = new TopicRequest(header.id, header.body, metadata);
                        message = request.message;
                    }
                    // Send response
                    out.write(message);
                    out.flush();
                }
            } catch (IOException e) {
                System.err.println("Error handling client: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    public static void runServer(Metadata metadata, int port) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server listening...");
            ExecutorService executor = Executors.newCachedThreadPool();
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket, metadata));
            }
        }
    }

    /**
     * Main method.
     */
    public static void main(String[] args) {
        int port = 9092;
        System.out.println("Logs from your program will appear here!");
        try {
            // Read metadata
            byte[] data = Files.readAllBytes(Paths.get(METADATA_LOG_PATH));
            Metadata metadata = new Metadata(data);
            System.out.println(metadata.getTopics());
            // Start server
            runServer(metadata, port);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}