import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

/**
 * Main class for a simple Kafka clone that supports the ApiVersions and DescribeTopicPartitions requests.
 * <p>
 * The broker accepts connections on port 9092, reads a fixed 12-byte header
 * (4 bytes message size (size of payload), 2 bytes api_key, 2 bytes api_version, 4 bytes correlation_id),
 * and sends a response in Kafka’s flexible (compact) message format.
 * <p>
 * For ApiVersions requests (api_key == 18):
 * <ul>
 *   <li>If the requested api_version is unsupported (< 0 or > 4) an error response with error_code 35 is returned.</li>
 *   <li>For api_version 0–3, a successful response is returned with error_code 0 and two ApiVersion entries
 *       (api_key 18, min_version 0, max_version 4; api_key 75, min_version 0, max_version 0).</li>
 *   <li>For api_version 4, a minimal successful response is returned with error_code 0 and no ApiVersion entries.</li>
 * </ul>
 * <p>
 * For DescribeTopicPartitions requests (api_key == 75, version 0):
 * <ul>
 *   <li>Responds with error_code 3 (UNKNOWN_TOPIC_OR_PARTITION), echoing the requested topic name,
 *       a zeroed topic_id, empty partitions, and tester-specific fields.</li>
 * </ul>
 */
public class Main {
    private static final String METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    private static MetadataReader metadataReader;

    /**
     * MetadataReader class for reading Kafka cluster metadata.
     */
    static class MetadataReader {
        private final Map<String, TopicMetadata> topics = new HashMap<>();

        public MetadataReader() {
            try {
                byte[] data = Files.readAllBytes(Paths.get(METADATA_LOG_PATH));
                parseMetadata(data);
            } catch (IOException e) {
                System.err.println("Error reading metadata file: " + e.getMessage());
            }
        }

        private void parseMetadata(byte[] data) {
            ByteParser parser = new ByteParser(data);

            // Parse batches
            while (!parser.isFinished()) {
                // Skip offset (8 bytes)
                parser.consume(8);

                // Read batch length
                byte[] batchLengthBytes = parser.consume(4);
                int batchLength = ByteBuffer.wrap(batchLengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();

                // Read batch data
                byte[] batchData = parser.consume(batchLength);
                parseBatch(batchData);
            }

            System.err.println("Loaded metadata for " + topics.size() + " topics");
            for (Map.Entry<String, TopicMetadata> entry : topics.entrySet()) {
                System.err.println("Topic: " + entry.getKey() + ", UUID: " + entry.getValue().topicId +
                                  ", Partitions: " + entry.getValue().partitions.size());
            }
        }

        private void parseBatch(byte[] batchData) {
            ByteParser parser = new ByteParser(batchData);

            // Skip batch header fields
            parser.consume(4); // partitionLeaderEpoch
            parser.consume(1); // magic
            parser.consume(4); // crc
            parser.consume(2); // attributes
            parser.consume(4); // lastOffsetDelta
            parser.consume(8); // firstTimestamp
            parser.consume(8); // maxTimestamp
            parser.consume(8); // producerId
            parser.consume(2); // producerEpoch
            parser.consume(4); // baseSequence

            // Read records count
            byte[] recordsCountBytes = parser.consume(4);
            int recordsCount = ByteBuffer.wrap(recordsCountBytes).order(ByteOrder.BIG_ENDIAN).getInt();

            // Parse records
            for (int i = 0; i < recordsCount && !parser.eof(); i++) {
                parseRecord(parser);
            }
        }

        private void parseRecord(ByteParser parser) {
            try {
                // Read record length
                int recordLength = parser.consumeVarInt(true);

                // Read record data
                byte[] recordData = parser.consume(recordLength);
                ByteParser recordParser = new ByteParser(recordData);

                // Skip record header fields
                recordParser.consume(1); // attributes
                recordParser.consumeVarInt(true); // timestampDelta
                recordParser.consumeVarInt(true); // offsetDelta

                // Skip key if present
                int keyLength = recordParser.consumeVarInt(true);
                if (keyLength > 0) {
                    recordParser.consume(keyLength);
                }

                // Read value
                int valueLength = recordParser.consumeVarInt(true);
                if (valueLength > 0) {
                    byte[] valueData = recordParser.consume(valueLength);
                    parseValue(valueData);
                }
            } catch (Exception e) {
                System.err.println("Error parsing record: " + e.getMessage());
            }
        }

        private void parseValue(byte[] valueData) {
            ByteParser parser = new ByteParser(valueData);

            // Read frame version and type
            parser.consume(1); // frameVersion
            byte[] typeBytes = parser.consume(1);
            int type = typeBytes[0] & 0xFF;

            // Process based on type
            if (type == 2) {
                // Topic metadata
                parseTopic(parser);
            } else if (type == 3) {
                // Partition metadata
                parsePartition(parser);
            }
        }

        private void parseTopic(ByteParser parser) {
            try {
                // Skip version
                parser.consume(1);

                // Read topic name
                int nameLength = parser.consumeVarInt(false) - 1;
                byte[] nameBytes = parser.consume(nameLength);
                String topicName = new String(nameBytes, StandardCharsets.UTF_8);

                // Read topic UUID
                byte[] uuidBytes = parser.consume(16);
                UUID topicId = bytesToUuid(uuidBytes);

                // Create topic metadata
                TopicMetadata metadata = new TopicMetadata(topicName, topicId);
                topics.put(topicName, metadata);

                System.err.println("Found topic: " + topicName + " with ID: " + topicId);
            } catch (Exception e) {
                System.err.println("Error parsing topic: " + e.getMessage());
            }
        }

        private void parsePartition(ByteParser parser) {
            try {
                // Skip version
                parser.consume(1);

                // Read partition ID
                byte[] partitionIdBytes = parser.consume(4);
                int partitionId = ByteBuffer.wrap(partitionIdBytes).order(ByteOrder.BIG_ENDIAN).getInt();

                // Read topic UUID
                byte[] uuidBytes = parser.consume(16);
                UUID topicId = bytesToUuid(uuidBytes);

                // Skip replica arrays
                int replicaArrayLength = parser.consumeVarInt(false) - 1;
                for (int i = 0; i < replicaArrayLength; i++) {
                    parser.consume(4);
                }

                int isrArrayLength = parser.consumeVarInt(false) - 1;
                List<Integer> isr = new ArrayList<>();
                for (int i = 0; i < isrArrayLength; i++) {
                    byte[] nodeIdBytes = parser.consume(4);
                    int nodeId = ByteBuffer.wrap(nodeIdBytes).order(ByteOrder.BIG_ENDIAN).getInt();
                    isr.add(nodeId);
                }

                // Skip removing/adding arrays
                int removingArrayLength = parser.consumeVarInt(false) - 1;
                for (int i = 0; i < removingArrayLength; i++) {
                    parser.consume(4);
                }

                int addingArrayLength = parser.consumeVarInt(false) - 1;
                for (int i = 0; i < addingArrayLength; i++) {
                    parser.consume(4);
                }

                // Read leader and epoch
                byte[] leaderIdBytes = parser.consume(4);
                int leaderId = ByteBuffer.wrap(leaderIdBytes).order(ByteOrder.BIG_ENDIAN).getInt();

                byte[] leaderEpochBytes = parser.consume(4);
                int leaderEpoch = ByteBuffer.wrap(leaderEpochBytes).order(ByteOrder.BIG_ENDIAN).getInt();

                // Find topic by UUID and add partition
                for (TopicMetadata topic : topics.values()) {
                    if (topic.topicId.equals(topicId)) {
                        PartitionMetadata partition = new PartitionMetadata(partitionId, leaderId, leaderEpoch);
                        partition.isr.addAll(isr);
                        topic.partitions.add(partition);
                        break;
                    }
                }
            } catch (Exception e) {
                System.err.println("Error parsing partition: " + e.getMessage());
            }
        }

        private UUID bytesToUuid(byte[] bytes) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            long high = bb.getLong();
            long low = bb.getLong();
            return new UUID(high, low);
        }

        public TopicMetadata getTopicMetadata(String topicName) {
            return topics.get(topicName);
        }

        public boolean topicExists(String topicName) {
            return topics.containsKey(topicName);
        }
    }

    /**
     * Topic metadata class.
     */
    static class TopicMetadata {
        public final String name;
        public final UUID topicId;
        public final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, UUID topicId) {
            this.name = name;
            this.topicId = topicId;
            this.partitions = new ArrayList<>();
        }
    }

    /**
     * Partition metadata class.
     */
    static class PartitionMetadata {
        public final int partitionIndex;
        public final int leaderId;
        public final int leaderEpoch;
        public final List<Integer> isr;

        public PartitionMetadata(int partitionIndex, int leaderId, int leaderEpoch) {
            this.partitionIndex = partitionIndex;
            this.leaderId = leaderId;
            this.leaderEpoch = leaderEpoch;
            this.isr = new ArrayList<>();
        }
    }

    /**
     * ByteParser class for parsing binary data.
     */
    static class ByteParser {
        private final byte[] data;
        private int index;
        private boolean finished;

        private static final int MSB_SET_MASK = 0b10000000;
        private static final int REMOVE_MSB_MASK = 0b01111111;

        public ByteParser(byte[] data) {
            this.data = data;
            this.index = 0;
            this.finished = false;
        }

        public boolean isFinished() {
            return finished;
        }

        public boolean eof() {
            return index >= data.length;
        }

        private void checkIsFinished() {
            finished = index >= data.length;
        }

        public byte[] read(int numBytes) {
            if (index + numBytes > data.length) {
                throw new IllegalArgumentException("Not enough bytes to read");
            }
            byte[] result = new byte[numBytes];
            System.arraycopy(data, index, result, 0, numBytes);
            return result;
        }

        public byte[] consume(int numBytes) {
            if (index + numBytes > data.length) {
                throw new IllegalArgumentException("Not enough bytes to read");
            }
            byte[] result = new byte[numBytes];
            System.arraycopy(data, index, result, 0, numBytes);
            index += numBytes;
            checkIsFinished();
            return result;
        }

        public int consumeVarInt(boolean signed) {
            int shift = 0;
            int value = 0;
            int aux = MSB_SET_MASK;
            int currentIndex = index;

            while ((aux & MSB_SET_MASK) != 0) {
                if (currentIndex >= data.length) {
                    throw new IllegalArgumentException("Not enough bytes to read variable int");
                }

                aux = data[currentIndex] & 0xFF;
                value += (aux & REMOVE_MSB_MASK) << shift;
                currentIndex++;
                shift += 7;
            }

            index = currentIndex;
            checkIsFinished();

            if (signed) {
                int lsb = value & 0x01;
                if (lsb != 0) {
                    value = -1 * ((value + 1) >> 1);
                } else {
                    value = value >> 1;
                }
            }

            return value;
        }
    }

    /**
     * Encodes a Java String into Kafka's COMPACT_STRING format (1-byte length + UTF-8 bytes).
     * A null string is encoded as INT16 -1.
     * @param s the string to encode.
     * @return the encoded bytes.
     */
    public static byte[] encodeKafkaString(String s) {
        if (s == null) {
            return new byte[]{(byte) 1}; // Length=1 (empty)
        }
        byte[] utf8Bytes = s.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[1 + utf8Bytes.length];
        result[0] = (byte) (utf8Bytes.length + 1);
        System.arraycopy(utf8Bytes, 0, result, 1, utf8Bytes.length);
        return result;
    }

    /**
     * Helper class to hold the incoming request header.
     */
    public static class FullRequestHeader {
        public int messageSize; // The total size of the message *following* the message_size field itself.
        public short apiKey;
        public short apiVersion;
        public int correlationId;
        public String clientId;
        public int clientIdFieldLengthBytes; // Bytes consumed by client_id: 2 for len + N for data, or 2 if null/empty.
        public int bodySize; // Calculated size of the actual request body payload (messageSize - commonHeaderParts - clientIdFieldLength)

        // Default constructor
        public FullRequestHeader() {}
    }

    // Helper class for readKafkaString results
    public static class KafkaStringReadResult {
        public final String value;
        public final int bytesRead; // Total bytes read from stream for this string field

        public KafkaStringReadResult(String value, int bytesRead) {
            this.value = value;
            this.bytesRead = bytesRead;
        }
    }

    /**
     * Reads exactly n bytes from the InputStream.
     * @param in the input stream.
     * @param n the number of bytes to read.
     * @return the read bytes.
     * @throws IOException if the connection is closed before n bytes are read.
     */
    public static byte[] readNBytes(InputStream in, int n) throws IOException {
        byte[] data = new byte[n];
        int totalRead = 0;
        while (totalRead < n) {
            int bytesRead = in.read(data, totalRead, n - totalRead);
            if (bytesRead == -1) {
                throw new IOException("Expected " + n + " bytes, got " + totalRead + " bytes");
            }
            totalRead += bytesRead;
        }
        System.err.println("readNBytes: Requested=" + n + ", Read=" + totalRead + ", Data=" + bytesToHex(data));
        return data;
    }

    // Helper for logging byte arrays as hex
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static KafkaStringReadResult readKafkaString(InputStream in) throws IOException {
        byte[] lengthBytes = readNBytes(in, 2); // readNBytes will print its own log
        short length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getShort();
        System.err.println("readKafkaString: Length bytes=" + bytesToHex(lengthBytes) + ", Length=" + length);

        if (length == -1) { // Nullable string is null
            return new KafkaStringReadResult(null, 2);
        }
        if (length < 0) { // Invalid length
            System.err.println("readKafkaString: Invalid negative length " + length + " (and not -1 for null). Treating as empty string.");
            return new KafkaStringReadResult("", 2);
        }

        byte[] stringPayloadBytes = readNBytes(in, length); // readNBytes will print its own log
        System.err.println("readKafkaString: Read " + length + " bytes for string payload, Data=" + bytesToHex(stringPayloadBytes));
        return new KafkaStringReadResult(new String(stringPayloadBytes, StandardCharsets.UTF_8), 2 + length);
    }

    /**
     * Reads the full Kafka request header from the socket.
     * <p>
     * Header layout:
     * <ul>
     *   <li>4 bytes: message_size (size of data following this field)</li>
     *   <li>2 bytes: api_key (INT16)</li>
     *   <li>2 bytes: api_version (INT16)</li>
     *   <li>4 bytes: correlation_id (INT32)</li>
     *   <li>client_id: STRING (INT16 length + UTF-8 bytes)</li>
     * </ul>
     * @param in the socket input stream.
     * @return the populated FullRequestHeader.
     * @throws IOException if reading fails.
     */
    public static FullRequestHeader readFullRequestHeader(InputStream in) throws IOException {
        FullRequestHeader frh = new FullRequestHeader();

        byte[] messageSizeBytes = readNBytes(in, 4);
        frh.messageSize = ByteBuffer.wrap(messageSizeBytes).order(ByteOrder.BIG_ENDIAN).getInt();

        byte[] commonHeaderPartsBytes = readNBytes(in, 8); // apiKey, apiVersion, correlationId
        ByteBuffer commonHeaderBuffer = ByteBuffer.wrap(commonHeaderPartsBytes).order(ByteOrder.BIG_ENDIAN);
        frh.apiKey = commonHeaderBuffer.getShort();
        frh.apiVersion = commonHeaderBuffer.getShort();
        frh.correlationId = commonHeaderBuffer.getInt();

        // Read ClientId using the new helper, mimicking Python's read_string call
        KafkaStringReadResult clientIdResult = readKafkaString(in);
        frh.clientId = clientIdResult.value;
        frh.clientIdFieldLengthBytes = clientIdResult.bytesRead;

        int commonHeaderSize = 8; // apiKey (2) + apiVersion (2) + correlationId (4)
        int headerSizeForBodyCalc = commonHeaderSize + frh.clientIdFieldLengthBytes; // Mimics python's header_size calculation
        frh.bodySize = frh.messageSize - headerSizeForBodyCalc;

        // Mimic Python's print statement at the end of read_request_header
        System.err.println("readFullRequestHeader: TotalSize=" + frh.messageSize +
                           ", HeaderSize=" + headerSizeForBodyCalc +
                           ", BodySize=" + frh.bodySize);
        return frh;
    }

    /**
     * Discards remaining bytes from the request.
     * @param in the socket input stream.
     * @param remaining the number of remaining bytes to discard.
     */
    public static void discardRemainingRequest(InputStream in, int remaining) throws IOException {
        while (remaining > 0) {
            byte[] buffer = new byte[Math.min(4096, remaining)];
            int read = in.read(buffer, 0, buffer.length); // Read up to the buffer's capacity (which is the chunk_size)
            if (read == -1) {
                throw new IOException("Unexpected end of stream while discarding " + remaining + " bytes");
            }
            remaining -= read;
        }
    }

    /**
     * Builds an API Versions response, mimicking the Python build_api_versions_response.
     * Handles success (v0–3, v4) and error (unsupported version) cases.
     * The response includes a flexible header (correlation_id + tag_buffer).
     * <p>
     * Success response body for api_version 0–3 (22 bytes):
     * <ul>
     *   <li>error_code: INT16 (2 bytes, value 0)</li>
     *   <li>api_keys: compact array length: 1 byte (value 3, meaning two elements + 1)</li>
     *   <li>An ApiVersion entry (7 bytes) containing:
     *        api_key: INT16 (value 18),
     *        min_version: INT16 (value 0),
     *        max_version: INT16 (value 4),
     *        entry TAG_BUFFER: 1 byte (0x00)
     *   </li>
     *   <li>A DescribeTopicPartitions entry (7 bytes) containing:
     *        api_key: INT16 (value 75),
     *        min_version: INT16 (value 0),
     *        max_version: INT16 (value 0),
     *        entry TAG_BUFFER: 1 byte (0x00)
     *   </li>
     *   <li>throttle_time_ms: INT32 (value 0)</li>
     *   <li>overall TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * Success response body for api_version 4 (8 bytes):
     * <ul>
     *   <li>error_code: INT16 (2 bytes, value 0)</li>
     *   <li>api_keys: compact array length: 1 byte (value 1, meaning zero entries + 1)</li>
     *   <li>throttle_time_ms: INT32 (value 0)</li>
     *   <li>overall TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * Error response body (8 bytes for unsupported version):
     * <ul>
     *  <li>error_code: INT16 (2 bytes, value 35)</li>
     *  <li>api_keys: compact array length: 1 byte (value 1, meaning zero entries + 1)</li>
     *  <li>throttle_time_ms: INT32 (value 0)</li>
     *  <li>overall TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * @param correlationId the correlation id from the request.
     * @param requestedApiVersion the api_version from the client's request.
     * @return the complete ApiVersions response bytes.
     */
    public static byte[] buildApiVersionsInternalResponse(int correlationId, short apiVersion) {
        ByteArrayOutputStream response = new ByteArrayOutputStream();

        try {
            // Check if the requested API version is supported
            boolean isSupported = apiVersion >= 0 && apiVersion <= 4;
            short errorCode = isSupported ? (short) 0 : (short) 35;

            // Calculate total size (will be filled in later)
            response.write(new byte[4]);

            // Response header: correlation ID
            response.write(ByteBuffer.allocate(4).putInt(correlationId).array());

            // Error code
            response.write(ByteBuffer.allocate(2).putShort(errorCode).array());

            if (isSupported) {
                // API keys array - compact array format
                // Value 3 means 2 elements (3-1=2)
                response.write(3);

                // First API key entry: ApiVersions (key 18)
                response.write(ByteBuffer.allocate(2).putShort((short) 18).array());
                response.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
                response.write(ByteBuffer.allocate(2).putShort((short) 4).array());  // max_version
                response.write(0);  // Tagged fields (empty)

                // Second API key entry: DescribeTopicPartitions (key 75)
                response.write(ByteBuffer.allocate(2).putShort((short) 75).array());
                response.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
                response.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // max_version
                response.write(0);  // Tagged fields (empty)
            } else {
                // Empty API keys array for error case
                response.write(1);  // Value 1 means 0 elements (1-1=0)
            }

            // Throttle time (ms)
            response.write(ByteBuffer.allocate(4).putInt(0).array());

            // Tagged fields (empty)
            response.write(0);

            // Fill in the total size at the beginning
            byte[] responseBytes = response.toByteArray();
            ByteBuffer.wrap(responseBytes, 0, 4).putInt(responseBytes.length - 4);

            return responseBytes;
        } catch (IOException e) {
            System.err.println("Error building ApiVersions response: " + e.getMessage());
            return new byte[0];
        }
    }

    /**
     * Class for handling DescribeTopicPartitions requests.
     */
    static class DescribeTopicPartitionsRequest {
        private final int correlationId;
        private String topicName;

        public DescribeTopicPartitionsRequest(int correlationId, byte[] body) {
            this.correlationId = correlationId;

            try {
                ByteParser parser = new ByteParser(body);

                // Topics array - compact array
                int topicsCount = parser.consumeVarInt(false) - 1;
                System.err.println("Number of topics: " + topicsCount);

                if (topicsCount > 0) {
                    // For simplicity, we're handling only the first topic for now

                    // Topic name - compact string
                    int topicNameLength = parser.consumeVarInt(false) - 1;
                    byte[] topicNameBytes = parser.consume(topicNameLength);
                    this.topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
                    System.err.println("Requested topic name: '" + this.topicName + "'");

                    // Skip partitions array if present (not needed for this stage)
                    if (!parser.eof()) {
                        // Assumes an array of partitions, even if empty.
                        int partitionArrayLength = parser.consumeVarInt(false) - 1;
                        System.err.println("Skipping partition array of length: " + partitionArrayLength);
                        // In a complete solution, you'd parse the partitions here if needed.
                        // But since the tester only sends a topic name, we don't need this yet.
                    }

                    // Skip cursor
                    if (!parser.eof()) {
                        parser.consume(1); // Cursor is a single byte
                    }
                } else {
                    this.topicName = ""; // Handle empty topic list
                    System.err.println("Empty topic list in request.");
                }

                // You might want to log the parsed information for debugging.
                System.err.println("Parsed DescribeTopicPartitions request: topicName='" + this.topicName + "'");

            } catch (Exception e) {
                System.err.println("Error parsing request: " + e.getMessage());
                e.printStackTrace();
            }

        }

        public byte[] buildResponse() {
            System.err.println("Building response for topic: '" + topicName + "'");

            ByteBuffer bodyBuffer = ByteBuffer.allocate(1024);
            bodyBuffer.order(ByteOrder.BIG_ENDIAN);

            // Response header
            bodyBuffer.putInt(correlationId);
            bodyBuffer.put((byte) 0); // Tagged fields

            // Response body
            bodyBuffer.putInt(0); // throttle_time_ms

            // Topics array - use 2 for compact array format (meaning 1 topic)
            bodyBuffer.put((byte) 2); // topics_array_length

            // Check if the topic exists in the metadata
            TopicMetadata metadata = null;
            boolean isKnown = false;

            // Only check if topic name is not empty
            if (!topicName.isEmpty()) {
                metadata = metadataReader.getTopicMetadata(topicName);
                isKnown = metadata != null;
                System.err.println("Topic '" + topicName + "' exists: " + isKnown +
                                  (isKnown ? ", UUID: " + metadata.topicId : ""));
            } else {
                System.err.println("Topic name is empty, returning unknown topic error");
            }

            short errorCode = isKnown ? (short) 0 : (short) 3;
            bodyBuffer.putShort(errorCode); // error_code

            // Topic name (compact string)
            byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
            bodyBuffer.put((byte) (topicNameBytes.length + 1)); // Length
            bodyBuffer.put(topicNameBytes); // Topic name

            // Topic ID (UUID)
            if (isKnown) {
                bodyBuffer.putLong(metadata.topicId.getMostSignificantBits());
                bodyBuffer.putLong(metadata.topicId.getLeastSignificantBits());
            } else {
                bodyBuffer.put(new byte[16]); // Zeroed UUID
            }

            // Is internal flag
            bodyBuffer.put((byte) 0);

            // Partitions array
            if (isKnown && !metadata.partitions.isEmpty()) {
                bodyBuffer.put((byte) 2); // partitions_array_length (1 partition + 1)
                PartitionMetadata partition = metadata.partitions.get(0);

                System.err.println("Including partition: index=" + partition.partitionIndex +
                                  ", leader=" + partition.leaderId +
                                  ", epoch=" + partition.leaderEpoch);

                bodyBuffer.putShort((short) 0); // error_code
                bodyBuffer.putInt(partition.partitionIndex); // partition_index
                bodyBuffer.putInt(partition.leaderId); // leader_id
                bodyBuffer.putInt(partition.leaderEpoch); // leader_epoch

                // replica_nodes (empty array)
                bodyBuffer.put((byte) 1);

                // isr_nodes (empty array)
                bodyBuffer.put((byte) 1);

                // eligible_leader_replicas (empty array)
                bodyBuffer.put((byte) 1);

                // last_known_elr (empty array)
                bodyBuffer.put((byte) 1);

                // offline_replicas (empty array)
                bodyBuffer.put((byte) 1);

                bodyBuffer.put((byte) 0); // partition tagged_fields
            } else {
                bodyBuffer.put((byte) 1); // empty partitions array
            }

            bodyBuffer.putInt(0x00000DF8); // topic_authorized_operations
            bodyBuffer.put((byte) 0); // topic_tag_buffer

            // Add cursor (null cursor)
            bodyBuffer.put((byte) 0xFF); // 0xFF indicates a null cursor

            // Tagged fields at end of response
            bodyBuffer.put((byte) 0); // response_tag_buffer

            // Create final message with size prefix
            byte[] responseBody = new byte[bodyBuffer.position()];
            bodyBuffer.flip();
            bodyBuffer.get(responseBody);

            // Dump the response body for debugging
            StringBuilder hexDump = new StringBuilder("Response body: ");
            for (byte b : responseBody) {
                hexDump.append(String.format("%02X ", b & 0xFF));
            }
            System.err.println(hexDump.toString());

            ByteBuffer finalBuffer = ByteBuffer.allocate(4 + responseBody.length);
            finalBuffer.order(ByteOrder.BIG_ENDIAN);
            finalBuffer.putInt(responseBody.length);
            finalBuffer.put(responseBody);

            byte[] response = finalBuffer.array();
            System.err.println("Response size: " + response.length + " bytes");

            return response;
        }
    }

    // Removed unused method

    /**
     * Handles client connections and processes requests.
     */
    private static void handleClient(Socket clientSocket) throws IOException {
        InputStream in = clientSocket.getInputStream();
        OutputStream out = clientSocket.getOutputStream();

        byte[] buffer = new byte[4096];
        int bytesRead;

        while ((bytesRead = in.read(buffer)) != -1) {
            try {
                // Parse the request header
                ByteBuffer headerBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
                headerBuffer.order(ByteOrder.BIG_ENDIAN);

                // Read message size (for logging only)
                int messageSize = headerBuffer.getInt();
                System.err.println("Message size: " + messageSize + " bytes");

                // Read API key
                short apiKey = headerBuffer.getShort();

                // Read API version
                short apiVersion = headerBuffer.getShort();

                // Read correlation ID
                int correlationId = headerBuffer.getInt();

                System.err.println("Received request: api_key=" + apiKey +
                                  ", api_version=" + apiVersion +
                                  ", correlation_id=" + correlationId);

                // Skip client ID (variable length)
                short clientIdLength = headerBuffer.getShort();
                if (clientIdLength > 0) {
                    byte[] clientIdBytes = new byte[clientIdLength];
                    headerBuffer.get(clientIdBytes);
                    String clientId = new String(clientIdBytes, StandardCharsets.UTF_8);
                    System.err.println("Client ID: " + clientId);
                }

                // Skip tagged fields
                if (headerBuffer.hasRemaining()) {
                    headerBuffer.get();
                }

                // Extract request body
                int bodyStart = headerBuffer.position();
                int bodyLength = bytesRead - bodyStart;
                byte[] body = new byte[bodyLength];
                if (bodyLength > 0) {
                    System.arraycopy(buffer, bodyStart, body, 0, bodyLength);

                    // Dump the full request for debugging
                    StringBuilder fullHexDump = new StringBuilder("Full request: ");
                    for (int i = 0; i < bytesRead; i++) {
                        fullHexDump.append(String.format("%02X ", buffer[i] & 0xFF));
                    }
                    System.err.println(fullHexDump.toString());
                }

                // Process based on API key
                byte[] response;
                if (apiKey == 18) { // ApiVersions
                    response = new ApiVersionsRequest(apiVersion, correlationId).buildResponse();
                } else if (apiKey == 75) { // DescribeTopicPartitions
                    response = new DescribeTopicPartitionsRequest(correlationId, body).buildResponse();
                } else {
                    System.err.println("Unknown API key: " + apiKey);
                    continue;
                }

                // Send response
                out.write(response);
                out.flush();
            } catch (Exception e) {
                System.err.println("Error handling request: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Class for handling ApiVersions requests.
     */
    static class ApiVersionsRequest {
        private final short apiVersion;
        private final int correlationId;

        public ApiVersionsRequest(short apiVersion, int correlationId) {
            this.apiVersion = apiVersion;
            this.correlationId = correlationId;
        }

        public byte[] buildResponse() {
            ByteBuffer bodyBuffer = ByteBuffer.allocate(64);
            bodyBuffer.order(ByteOrder.BIG_ENDIAN);

            // Response header
            bodyBuffer.putInt(correlationId);
            bodyBuffer.put((byte) 0); // Tagged fields

            // Error code
            boolean isSupported = apiVersion >= 0 && apiVersion <= 4;
            short errorCode = isSupported ? (short) 0 : (short) 35;
            bodyBuffer.putShort(errorCode);

            // API keys array
            if (isSupported) {
                bodyBuffer.put((byte) 3); // 3 means 2 elements (3-1=2)

                // ApiVersions (key 18)
                bodyBuffer.putShort((short) 18);
                bodyBuffer.putShort((short) 0); // min_version
                bodyBuffer.putShort((short) 4); // max_version
                bodyBuffer.put((byte) 0); // tag buffer

                // DescribeTopicPartitions (key 75)
                bodyBuffer.putShort((short) 75);
                bodyBuffer.putShort((short) 0); // min_version
                bodyBuffer.putShort((short) 0); // max_version
                bodyBuffer.put((byte) 0); // tag buffer
            } else {
                bodyBuffer.put((byte) 1); // 1 means 0 elements (1-1=0)
            }

            // Throttle time and tag buffer
            bodyBuffer.putInt(0); // throttle_time_ms
            bodyBuffer.put((byte) 0); // tag buffer

            // Create final message with size prefix
            byte[] responseBody = new byte[bodyBuffer.position()];
            bodyBuffer.flip();
            bodyBuffer.get(responseBody);

            ByteBuffer finalBuffer = ByteBuffer.allocate(4 + responseBody.length);
            finalBuffer.order(ByteOrder.BIG_ENDIAN);
            finalBuffer.putInt(responseBody.length);
            finalBuffer.put(responseBody);

            return finalBuffer.array();
        }
    }

    /**
     * Runs the server on the specified port.
     * <p>
     * This method continuously accepts new client connections.
     * For each connection, a new thread is spawned to handle sequential requests from that client.
     * @param port the port to listen on.
     * @param metadata the metadata reader instance.
     * @throws IOException if an I/O error occurs.
     */
    public static void runServer(int port, MetadataReader metadata) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.err.println("Server is listening on port " + port);
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
                try {
                    handleClient(clientSocket);
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
        } finally {
            serverSocket.close();
        }
    }

    /**
     * Main entry point.
     * @param args command-line arguments (optional port number).
     */
    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");

        try {
            // Initialize metadata reader
            metadataReader = new MetadataReader();

            // Start server
            runServer(9092, metadataReader);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
