import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * BaseKafka class with utility methods for Kafka protocol handling.
 */
abstract class BaseKafka {
    protected static final byte[] TAG_BUFFER = new byte[]{0x00};
    protected static final byte[] DEFAULT_THROTTLE_TIME = ByteBuffer.allocate(4).putInt(0).array();
    protected static final Map<String, byte[]> ERRORS = new HashMap<>();

    static {
        ERRORS.put("ok", ByteBuffer.allocate(2).putShort((short) 0).array());
        ERRORS.put("error", ByteBuffer.allocate(2).putShort((short) 35).array());
    }

    protected byte[] createMessage(byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.length);
        buffer.put(message);
        return buffer.array();
    }

    protected byte[] removeTagBuffer(byte[] buffer) {
        return Arrays.copyOfRange(buffer, 1, buffer.length);
    }

    protected StringResult parseString(byte[] buffer) {
        if (buffer.length < 2) {
            throw new IllegalArgumentException("Buffer too short for string length");
        }
        short length = ByteBuffer.wrap(buffer, 0, 2).order(ByteOrder.BIG_ENDIAN).getShort();
        if (length < 0) {
            return new StringResult("", Arrays.copyOfRange(buffer, 2, buffer.length));
        }
        if (buffer.length < 2 + length) {
            throw new IllegalArgumentException("Buffer too short for string data");
        }
        String str = new String(buffer, 2, length, StandardCharsets.UTF_8);
        return new StringResult(str, Arrays.copyOfRange(buffer, 2 + length, buffer.length));
    }

    protected static class StringResult {
        public final String value;
        public final byte[] remaining;

        public StringResult(String value, byte[] remaining) {
            this.value = value;
            this.remaining = remaining;
        }
    }

    protected byte[] parseArray(byte[] buffer, java.util.function.Consumer<byte[]> func) {
        if (buffer.length < 1) {
            throw new IllegalArgumentException("Buffer too short for array length");
        }
        int arrLength = (buffer[0] & 0xFF) - 1;
        byte[] arrBuffer = Arrays.copyOfRange(buffer, 1, buffer.length);
        for (int i = 0; i < arrLength; i++) {
            if (arrBuffer.length < 1) {
                throw new IllegalArgumentException("Buffer too short for array item");
            }
            int itemLength = (arrBuffer[0] & 0xFF);
            if (arrBuffer.length < 1 + itemLength) {
                throw new IllegalArgumentException("Buffer too short for item data");
            }
            byte[] itemBuffer = Arrays.copyOfRange(arrBuffer, 1, 1 + itemLength);
            func.accept(itemBuffer);
            arrBuffer = Arrays.copyOfRange(arrBuffer, 1 + itemLength, arrBuffer.length);
        }
        return arrBuffer;
    }
}

/**
 * KafkaHeader class to parse request headers.
 */
class KafkaHeader extends BaseKafka {
    public final byte[] length;
    public final byte[] key;
    public final int keyInt;
    public final byte[] version;
    public final int versionInt;
    public final byte[] id;
    public final String client;
    public final byte[] body;

    public KafkaHeader(byte[] data) {
        if (data.length < 12) {
            throw new IllegalArgumentException("Data too short for header");
        }
        this.length = Arrays.copyOfRange(data, 0, 4);
        this.key = Arrays.copyOfRange(data, 4, 6);
        this.keyInt = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN).getShort();
        this.version = Arrays.copyOfRange(data, 6, 8);
        this.versionInt = ByteBuffer.wrap(version).order(ByteOrder.BIG_ENDIAN).getShort();
        this.id = Arrays.copyOfRange(data, 8, 12);
        StringResult clientResult = parseString(Arrays.copyOfRange(data, 12, data.length));
        this.client = clientResult.value;
        byte[] buffer = removeTagBuffer(clientResult.remaining);
        this.body = buffer;
    }
}

/**
 * ApiRequest class for handling ApiVersions requests (api_key 18).
 */
class ApiRequest extends BaseKafka {
    private final int versionInt;
    private final byte[] id;

    public ApiRequest(int versionInt, byte[] id) {
        this.versionInt = versionInt;
        this.id = id;
    }

    public byte[] getMessage() {
        return createMessage(constructMessage());
    }

    protected byte[] constructMessage() {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try {
            body.write(id);
            body.write(TAG_BUFFER);
            body.write(errorHandler());
            if (versionInt >= 0 && versionInt <= 3) {
                body.write((byte) 3);
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 18).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 4).array());
                body.write((byte) 0);
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 75).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                body.write(ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 0).array());
                body.write((byte) 0);
            } else {
                body.write((byte) 1);
            }
            body.write(DEFAULT_THROTTLE_TIME);
            body.write(TAG_BUFFER);
            return body.toByteArray();
        } catch (IOException e) {
            System.err.println("Error constructing ApiVersions message: " + e.getMessage());
            return new byte[0];
        }
    }

    protected byte[] errorHandler() {
        return (versionInt >= 0 && versionInt <= 4) ? ERRORS.get("ok") : ERRORS.get("error");
    }
}

/**
 * DescribeTopicPartitionsRequest class for handling DescribeTopicPartitions requests (api_key 75).
 */
class DescribeTopicPartitionsRequest extends BaseKafka {
    private final byte[] id;
    private final byte[] body;
    private final String[] topics;
    private final byte[] cursor;
    private final Map<byte[], Metadata.TopicInfo> availableTopics;
    private final Map<byte[], Metadata.PartitionInfo> partitions;

    public DescribeTopicPartitionsRequest(byte[] id, byte[] body, Metadata metadata) {
        this.id = id;
        this.body = body;
        this.availableTopics = metadata.getTopics();
        this.partitions = metadata.getPartitions();
        java.util.ArrayList<String> topicList = new java.util.ArrayList<>();
        byte[] buffer = parseArray(body, item -> topicList.add(new String(item, StandardCharsets.UTF_8)));
        this.topics = topicList.toArray(new String[0]);
        this.cursor = Arrays.copyOfRange(buffer, 0, 1);
    }

    public byte[] getMessage() {
        return createMessage(constructMessage());
    }

    protected byte[] constructMessage() {
        ByteArrayOutputStream message = new ByteArrayOutputStream();
        try {
            message.write(id);
            message.write(TAG_BUFFER);
            message.write(DEFAULT_THROTTLE_TIME);
            message.write((byte) (topics.length + 1));
            if (topics.length > 0) {
                message.write(createTopicItem(topics[0].getBytes(StandardCharsets.UTF_8)));
            }
            message.write((byte) 0xFF);
            message.write(TAG_BUFFER);
            return message.toByteArray();
        } catch (IOException e) {
            System.err.println("Error constructing DescribeTopicPartitions message: " + e.getMessage());
            return new byte[0];
        }
    }

    protected byte[] createTopicItem(byte[] topic) {
        ByteArrayOutputStream topicBuffer = new ByteArrayOutputStream();
        try {
            boolean available = availableTopics.containsKey(topic);
            topicBuffer.write(available ? ERRORS.get("ok") : ByteBuffer.allocate(2).putShort((short) 3).array());
            topicBuffer.write((byte) (topic.length + 1));
            topicBuffer.write(topic);
            if (available) {
                UUID uuid = availableTopics.get(topic).uuid;
                topicBuffer.write(ByteBuffer.allocate(16)
                        .putLong(uuid.getMostSignificantBits())
                        .putLong(uuid.getLeastSignificantBits())
                        .array());
            } else {
                topicBuffer.write(new byte[16]);
            }
            topicBuffer.write((byte) 0);
            if (available && !availableTopics.get(topic).partitions.isEmpty()) {
                topicBuffer.write((byte) (availableTopics.get(topic).partitions.size() + 1));
                for (byte[] id : availableTopics.get(topic).partitions) {
                    topicBuffer.write(addPartition(partitions.get(id)));
                }
            } else {
                topicBuffer.write((byte) 1);
            }
            topicBuffer.write(ByteBuffer.allocate(4).putInt(0x00000DF8).array());
            topicBuffer.write(TAG_BUFFER);
            return topicBuffer.toByteArray();
        } catch (IOException e) {
            System.err.println("Error creating topic item: " + e.getMessage());
            return new byte[0];
        }
    }

    protected byte[] addPartition(Metadata.PartitionInfo partition) {
        ByteArrayOutputStream ret = new ByteArrayOutputStream();
        try {
            ret.write(ERRORS.get("ok"));
            ret.write(partition.id);
            ret.write(ByteBuffer.allocate(4).putInt(partition.leader).array());
            ret.write(ByteBuffer.allocate(4).putInt(partition.leaderEpoch).array());
            ret.write((byte) 1);
            ret.write((byte) 1);
            ret.write((byte) 1);
            ret.write((byte) 1);
            ret.write((byte) 1);
            ret.write((byte) 0);
            return ret.toByteArray();
        } catch (IOException e) {
            System.err.println("Error adding partition: " + e.getMessage());
            return new byte[0];
        }
    }
}

/**
 * Main class for the Kafka clone server.
 */
public class Main {
    private static final String METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    private static void handleClient(Socket clientSocket, Metadata metadata) throws IOException {
        try (InputStream in = clientSocket.getInputStream(); OutputStream out = clientSocket.getOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                byte[] data = Arrays.copyOf(buffer, bytesRead);
                KafkaHeader header = new KafkaHeader(data);
                byte[] message;
                if (header.keyInt == 18) {
                    message = new ApiRequest(header.versionInt, header.id).getMessage();
                } else if (header.keyInt == 75) {
                    message = new DescribeTopicPartitionsRequest(header.id, header.body, metadata).getMessage();
                } else {
                    System.err.println("Unknown API key: " + header.keyInt);
                    continue;
                }
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

    public static void runServer(int port, Metadata metadata) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.err.println("Server is listening on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
                new Thread(() -> {
                    try {
                        handleClient(clientSocket, metadata);
                    } catch (IOException e) {
                        System.err.println("Error in client thread: " + e.getMessage());
                    }
                }).start();
            }
        }
    }

    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");
        try {
            byte[] data = Files.readAllBytes(new File(METADATA_LOG_PATH).toPath());
            Metadata metadata = new Metadata(data);
            System.err.println("Loaded metadata: " + metadata.getTopics());
            runServer(9092, metadata);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}