import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Main {

    public static byte[] encodeKafkaString(String s) {
        if (s == null) {
            return ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) -1).array();
        }
        byte[] utf8Bytes = s.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + utf8Bytes.length).order(ByteOrder.BIG_ENDIAN).putShort((short) utf8Bytes.length).put(utf8Bytes).array();
    }

    public static class FullRequestHeader {
        public int messageSize;
        public short apiKey;
        public short apiVersion;
        public int correlationId;
        public String clientId;
        public int clientIdFieldLengthBytes;
        public int bodySize;

        public FullRequestHeader() {}
    }

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
        return data;
    }

    public static FullRequestHeader readFullRequestHeader(InputStream in) throws IOException {
        FullRequestHeader frh = new FullRequestHeader();

        byte[] messageSizeBytes = readNBytes(in, 4);
        frh.messageSize = ByteBuffer.wrap(messageSizeBytes).order(ByteOrder.BIG_ENDIAN).getInt();

        byte[] commonHeaderPartsBytes = readNBytes(in, 8);
        ByteBuffer commonHeaderBuffer = ByteBuffer.wrap(commonHeaderPartsBytes).order(ByteOrder.BIG_ENDIAN);
        frh.apiKey = commonHeaderBuffer.getShort();
        frh.apiVersion = commonHeaderBuffer.getShort();
        frh.correlationId = commonHeaderBuffer.getInt();

        byte[] clientIdLenBytes = readNBytes(in, 2);
        short clientIdLenRaw = ByteBuffer.wrap(clientIdLenBytes).order(ByteOrder.BIG_ENDIAN).getShort();

        if (clientIdLenRaw == -1) {
            frh.clientId = null;
            frh.clientIdFieldLengthBytes = 2;
        } else if (clientIdLenRaw < 0) {
            System.err.println("Warning: Invalid Client ID length: " + clientIdLenRaw + ". Treating as empty.");
            frh.clientId = "";
            frh.clientIdFieldLengthBytes = 2;
        } else {
            byte[] clientIdValueBytes = readNBytes(in, clientIdLenRaw);
            frh.clientId = new String(clientIdValueBytes, StandardCharsets.UTF_8);
            frh.clientIdFieldLengthBytes = 2 + clientIdLenRaw;
        }

        int commonHeaderSize = 8;
        frh.bodySize = frh.messageSize - commonHeaderSize - frh.clientIdFieldLengthBytes;

        return frh;
    }

    public static void discardRemainingRequest(InputStream in, int remaining) throws IOException {
        while (remaining > 0) {
            byte[] buffer = new byte[Math.min(4096, remaining)];
            int read = in.read(buffer, 0, Math.min(buffer.length, remaining));
            if (read == -1) {
                throw new IOException("Unexpected end of stream while discarding " + remaining + " bytes");
            }
            remaining -= read;
        }
    }

    public static void buildApiVersionsResponse(FullRequestHeader fullHeader, OutputStream out) throws IOException {
        if (fullHeader.apiVersion < 0 || fullHeader.apiVersion > 4) {
            System.err.println("Unsupported api_version " + fullHeader.apiVersion + ", sending error response.");
            byte[] errorResponse = buildErrorResponse(fullHeader.correlationId, (short) 35);
            out.write(errorResponse);
        } else {
            System.err.println("Sending success response for api_version " + fullHeader.apiVersion);
            byte[] successResponse = buildSuccessResponse(fullHeader.correlationId);
            out.write(successResponse);
        }
        out.flush();
    }

    public static byte[] buildErrorResponse(int correlationId, short errorCode) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(4 + 8);
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.put((byte) 0);
        buffer.putInt(0);
        buffer.put((byte) 0);
        return buffer.array();
    }

    public static byte[] buildSuccessResponse(int correlationId) {
        int bodySize = 22;
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + bodySize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(4 + bodySize);
        buffer.putInt(correlationId);
        buffer.putShort((short) 0);
        buffer.put((byte) 3);
        buffer.putShort((short) 18);
        buffer.putShort((short) 0);
        buffer.putShort((short) 4);
        buffer.put((byte) 0);
        buffer.putShort((short) 75);
        buffer.putShort((short) 0);
        buffer.putShort((short) 0);
        buffer.put((byte) 0);
        buffer.putInt(0);
        buffer.put((byte) 0);
        return buffer.array();
    }

    public static String parseDescribeTopicPartitionsRequest(InputStream in, int remaining) throws IOException {
        int bytesConsumed = 0;
        String firstTopicName = null;

        if (remaining < 2) {
            System.err.println("P_DTP_R: Body too small for topicsCount (" + remaining + " bytes).");
            if (remaining > 0) discardRemainingRequest(in, remaining);
            return "";
        }
        byte[] topicsCountBytes = readNBytes(in, 2);
        bytesConsumed += 2;
        short topicsCount = ByteBuffer.wrap(topicsCountBytes).order(ByteOrder.BIG_ENDIAN).getShort();

        if (topicsCount < 0) {
            System.err.println("P_DTP_R: Invalid topics_count " + topicsCount + ".");
            if (remaining - bytesConsumed > 0) discardRemainingRequest(in, remaining - bytesConsumed);
            return "";
        }

        System.err.println("P_DTP_R: Expecting " + topicsCount + " topics.");

        for (int i = 0; i < topicsCount; i++) {
            if (remaining - bytesConsumed < 2) {
                System.err.println("P_DTP_R: Not enough data for topic_name length (topic " + (i+1) + "). Remaining: " + (remaining - bytesConsumed));
                break;
            }
            byte[] nameLenBytes = readNBytes(in, 2);
            bytesConsumed += 2;
            short nameLen = ByteBuffer.wrap(nameLenBytes).order(ByteOrder.BIG_ENDIAN).getShort();

            String currentTopicNameStr = "";
            if (nameLen < 0) {
                System.err.println("P_DTP_R: Invalid topic_name length " + nameLen + " for topic " + (i+1) + ". STRING requires >= 0.");
            } else if (nameLen > 0) {
                if (nameLen > remaining - bytesConsumed) {
                    System.err.println("P_DTP_R: Stated topic_name length " + nameLen + " for topic " + (i+1) + " exceeds remaining body bytes " + (remaining - bytesConsumed) + ". Malformed.");
                    if (firstTopicName == null) firstTopicName = "";
                    break;
                }
                byte[] namePayloadBytes = readNBytes(in, nameLen);
                bytesConsumed += nameLen;
                try {
                    currentTopicNameStr = new String(namePayloadBytes, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    System.err.println("P_DTP_R: Topic_name for topic " + (i+1) + " had decode error. Treating as empty.");
                }
            }

            if (firstTopicName == null) {
                firstTopicName = currentTopicNameStr;
            }

            if (remaining - bytesConsumed < 4) {
                System.err.println("P_DTP_R: Not enough data for partitionsCount (topic " + (i+1) + "). Remaining: " + (remaining - bytesConsumed));
                break;
            }
            byte[] partitionsCountBytes = readNBytes(in, 4);
            bytesConsumed += 4;
            int partitionsCount = ByteBuffer.wrap(partitionsCountBytes).order(ByteOrder.BIG_ENDIAN).getInt();

            if (partitionsCount < 0) {
                System.err.println("P_DTP_R: Invalid partitions_count " + partitionsCount + " for topic " + (i+1) + ".");
                break;
            }

            int bytesForPartitionIds = partitionsCount * 4;
            if (bytesForPartitionIds > 0) {
                if (bytesForPartitionIds > remaining - bytesConsumed) {
                    System.err.println("P_DTP_R: Stated " + bytesForPartitionIds + " bytes for partition IDs (topic " + (i+1) + ") exceeds remaining body bytes " + (remaining - bytesConsumed) + ". Malformed.");
                    break;
                }
                discardRemainingRequest(in, bytesForPartitionIds);
                bytesConsumed += bytesForPartitionIds;
            }
        }

        int unparsedBytes = remaining - bytesConsumed;
        if (unparsedBytes > 0) {
            System.err.println("P_DTP_R: Discarding " + unparsedBytes + " unparsed bytes from request body.");
            discardRemainingRequest(in, unparsedBytes);
        } else if (unparsedBytes < 0) {
            System.err.println("P_DTP_R: WARNING - Consumed " + bytesConsumed + " bytes, but initial remaining was " + remaining + ".");
        }

        String parsedTopicName = (firstTopicName != null) ? firstTopicName : "";
        System.err.println("P_DTP_R: Parsed first topic='" + parsedTopicName + "'");
        return parsedTopicName;
    }

    public static byte[] buildDescribeTopicPartitionsResponse(int correlationId, String topic) throws IOException {
        if (topic == null) {
            topic = "";
        }
        System.err.println("build_describe_topic_partitions_response: topic_name='" + topic + "' for response.");

        byte[] errorCodeBytes = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN).putShort((short) 3).array();
        byte[] topicNameEncoded = encodeKafkaString(topic);
        byte[] topicId = new byte[16];
        byte[] partitionsCountBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(0).array();

        byte[] responseBody = concatenateByteArrays(
            errorCodeBytes,
            topicNameEncoded,
            topicId,
            partitionsCountBytes
        );

        byte[] responseHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(correlationId).array();
        int messageSize = responseHeader.length + responseBody.length;

        ByteBuffer buffer = ByteBuffer.allocate(4 + messageSize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(messageSize);
        buffer.put(responseHeader);
        buffer.put(responseBody);
        return buffer.array();
    }

    private static byte[] concatenateByteArrays(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            totalLength += array.length;
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static void handleClient(Socket clientSocket) {
        try {
            InputStream clientInputStream = clientSocket.getInputStream();
            OutputStream clientOutputStream = clientSocket.getOutputStream();
            while (true) {
                FullRequestHeader fullHeader;
                try {
                    fullHeader = readFullRequestHeader(clientInputStream);
                } catch (IOException ioe) {
                    System.err.println("IOException while reading header, closing connection: " + ioe.getMessage());
                    break;
                } catch (Exception e) {
                    System.err.println("Error reading full request header: " + e.getMessage());
                    break;
                }
                System.err.println("Received correlation_id: " + fullHeader.correlationId
                        + ", requested api_version: " + fullHeader.apiVersion
                        + ", api_key: " + fullHeader.apiKey);
                System.err.println("Received request messageSize (payload after size field): " + fullHeader.messageSize);
                System.err.println("Client ID: '" + fullHeader.clientId + "', Client ID field length: " + fullHeader.clientIdFieldLengthBytes);
                System.err.println("Calculated request bodySize: " + fullHeader.bodySize);

                if (fullHeader.bodySize < 0) {
                    System.err.println("Error: Calculated negative bodySize (" + fullHeader.bodySize + "). Protocol error or parsing issue.");
                    break;
                }

                if (fullHeader.apiKey == 18) {
                    if (fullHeader.bodySize > 0) {
                        discardRemainingRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Discarded " + fullHeader.bodySize + " bytes from ApiVersions request body.");
                    }
                    buildApiVersionsResponse(fullHeader, clientOutputStream);
                } else if (fullHeader.apiKey == 75) {
                    String topic;
                    try {
                        topic = parseDescribeTopicPartitionsRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Parsed topic: " + topic);
                    } catch (IOException e) {
                        System.err.println("Error parsing DescribeTopicPartitions request: " + e.getMessage());
                        topic = "";
                    }
                    byte[] response = buildDescribeTopicPartitionsResponse(fullHeader.correlationId, topic);
                    clientOutputStream.write(response);
                    clientOutputStream.flush();
                    System.err.println("Sent DescribeTopicPartitions response (" + response.length + " bytes)");
                } else {
                    System.err.println("Unknown api_key " + fullHeader.apiKey + ", skipping.");
                    if (fullHeader.bodySize > 0) {
                        discardRemainingRequest(clientInputStream, fullHeader.bodySize);
                        System.err.println("Discarded " + fullHeader.bodySize + " bytes from unknown request.");
                    }
                }
            }
        } catch (Throwable t) {
            System.err.println("Error handling client: " + t.getMessage());
        } finally {
            try {
                clientSocket.shutdownOutput();
            } catch (IOException e) {}
            try {
                clientSocket.close();
            } catch (IOException e) {}
            System.err.println("Client connection closed.");
        }
    }

    public static void runServer(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.err.println("Server is listening on port " + port);
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } finally {
            serverSocket.close();
        }
    }

    public static void main(String[] args) {
        int port = 9092;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number, using default port 9092.");
            }
        }
        System.err.println("Starting server on port " + port);
        System.err.println("Logs from your program will appear here!");
        try {
            runServer(port);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}