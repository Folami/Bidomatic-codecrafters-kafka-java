import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Main class for a simple Kafka clone that supports the ApiVersions request.
 * <p>
 * The broker accepts connections on port 9092, reads a fixed 12-byte header 
 * (4 bytes message size (unused), 2 bytes api_key, 2 bytes api_version, 4 bytes correlation_id),
 * and sends a response in Kafka’s flexible (compact) message format.
 * <p>
 * For ApiVersions requests (api_key == 18):
 * <ul>
 *   <li>If the requested api_version is unsupported (< 0 or > 4) an error response with error_code 35 is returned.</li>
 *   <li>Otherwise, a successful response is returned with error_code 0 and one ApiVersion entry 
 *       (api_key 18, min_version 0, max_version 4).</li>
 * </ul>
 * <p>
 * The successful response body layout (15 bytes) is as follows:
 * <pre>
 *   error_code                : INT16 (2 bytes)
 *   api_keys (compact array)  : 1 byte (length = 2, i.e. one element + 1)
 *     - Entry:
 *         api_key           : INT16 (2 bytes)  (value 18)
 *         min_version       : INT16 (2 bytes)  (value 0)
 *         max_version       : INT16 (2 bytes)  (value 4)
 *         entry TAG_BUFFER  : 1 byte  (0x00 for empty)
 *   throttle_time_ms          : INT32 (4 bytes, value 0)
 *   response TAG_BUFFER       : 1 byte  (0x00 for empty)
 * </pre>
 * <p>
 * The overall message after the message_length field is:
 * 4 bytes (correlation_id) + 15 bytes (body) = 19 bytes.
 * Combined with the 4-byte message_length field, the total transmission is 23 bytes.
 */
public class Main {

    /**
     * Helper class to hold the incoming request header.
     */
    public static class RequestHeader {
        public int requestSize;
        public short apiKey;
        public short apiVersion;
        public int correlationId;

        public RequestHeader(int requestSize, short apiKey, short apiVersion, int correlationId) {
            this.requestSize = requestSize;
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
            this.correlationId = correlationId;
        }
    }

    /**
     * Reads exactly n bytes from the InputStream.
     * 
     * @param in the input stream.
     * @param n  the number of bytes to read.
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
        return data;
    }

    /**
     * Reads the fixed 12-byte header from the socket.
     * <p>
     * Header layout:
     * <ul>
     *   <li>4 bytes: request message size (size of data following this field)</li>
     *   <li>2 bytes: api_key (INT16)</li>
     *   <li>2 bytes: api_version (INT16)</li>
     *   <li>4 bytes: correlation_id (INT32)</li>
     * </ul>
     * 
     * @param in the socket input stream.
     * @return the populated RequestHeader.
     * @throws IOException if reading fails.
     */
    public static RequestHeader readRequestHeader(InputStream in) throws IOException {
        // Read size field (4 bytes)
        byte[] sizeBytes = readNBytes(in, 4);
        ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
        sizeBuffer.order(ByteOrder.BIG_ENDIAN);
        int messageSize = sizeBuffer.getInt();

        // Read the rest of the header (8 bytes)
        byte[] headerBytes = readNBytes(in, 8);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerBytes);
        headerBuffer.order(ByteOrder.BIG_ENDIAN);
        short apiKey = headerBuffer.getShort();
        short apiVersion = headerBuffer.getShort();
        int correlationId = headerBuffer.getInt();

        return new RequestHeader(messageSize, apiKey, apiVersion, correlationId);
    }

    /**
     * Builds an error response when the requested API version is unsupported.
     * <p>
     * Error response body layout (8 bytes):
     * <ul>
     *   <li>error_code: INT16 (2 bytes)</li>
     *   <li>compact array: 1 byte (0 means no elements)</li>
     *   <li>throttle_time_ms: INT32 (4 bytes, 0)</li>
     *   <li>response TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * The overall message length (excluding the 4-byte length field) is
     * 4 (correlation_id) + 8 (body) = 12 bytes.
     * 
     * @param correlationId the correlation id from the request.
     * @param errorCode     the error code to return.
     * @return the complete error response bytes.
     */
    public static byte[] buildErrorResponse(int correlationId, short errorCode) {
        // Total response = 4 (message_length field) + 4 (correlation_id) + 8 (body) = 16 bytes.
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(4 + 8); // message_length = 12 bytes (correlation_id + body)
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.put((byte) 0); // compact array: no elements
        buffer.putInt(0);     // throttle_time_ms
        buffer.put((byte) 0); // empty response TAG_BUFFER
        return buffer.array();
    }

    /**
     * Builds a successful API Versions response.
     * <p>
     * Successful response body layout (15 bytes):
     * <ul>
     *   <li>error_code: INT16 (2 bytes, value 0)</li>
     *   <li>api_keys: compact array length (1 byte, value = 2 for one element + 1)</li>
     *   <li>One ApiVersion entry (7 bytes):
     *     <ul>
     *       <li>api_key: INT16 (2 bytes, value 18)</li>
     *       <li>min_version: INT16 (2 bytes, value 0)</li>
     *       <li>max_version: INT16 (2 bytes, value 4)</li>
     *       <li>entry TAG_BUFFER: 1 byte (0x00)</li>
     *     </ul>
     *   </li>
     *   <li>throttle_time_ms: INT32 (4 bytes, value 0)</li>
     *   <li>response TAG_BUFFER: 1 byte (0x00)</li>
     * </ul>
     * Overall, the message (after the 4-byte message_length field) comprises:
     * 4 (correlation_id) + 15 (body) = 19 bytes. Thus, total response size is 23 bytes.
     * 
     * @param correlationId the correlation id from the request.
     * @return the complete success response bytes.
     */
    public static byte[] buildSuccessResponse(int correlationId) {
        ByteBuffer buffer = ByteBuffer.allocate(23);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(4 + 15); // message_length = 19 bytes (correlation_id + body)
        buffer.putInt(correlationId);
        buffer.putShort((short) 0);   // error_code = 0
        buffer.put((byte) 2);         // compact array length = 2 (one element + 1)
        buffer.putShort((short) 18);  // api_key = 18 (API_VERSIONS)
        buffer.putShort((short) 0);   // min_version = 0
        buffer.putShort((short) 4);   // max_version = 4
        buffer.put((byte) 0);         // entry TAG_BUFFER = empty
        buffer.putInt(0);             // throttle_time_ms = 0
        buffer.put((byte) 0);         // overall TAG_BUFFER = empty
        return buffer.array();
    }

    /**
     * Handles an individual client connection.
     */
    public static void handleClient(Socket clientSocket) throws IOException {
        InputStream in = clientSocket.getInputStream();
        OutputStream out = clientSocket.getOutputStream();

        // Loop processing sequential requests on the same connection.
        while (true) {
            RequestHeader header = null;
            try {
                header = readRequestHeader(in);
            } catch (IOException ioe) {
                // Likely end of stream (client closed connection).
                break;
            }
            System.err.println("Received correlation_id: " + header.correlationId +
                    ", requested api_version: " + header.apiVersion);
            System.err.println("Received request size: " + header.requestSize);

            // Discard remaining request bytes.
            // The request_size field specifies size of data after itself,
            // we've read 8 bytes of header, so remaining = request_size - 8
            int remainingBytes = header.requestSize - 8;
            while (remainingBytes > 0) {
                byte[] discardBuffer = new byte[Math.min(4096, remainingBytes)];
                int read = in.read(discardBuffer);
                if (read == -1)
                    break;
                remainingBytes -= read;
            }

            // Process ApiVersions request.
            if (header.apiKey == 18) {
                if (header.apiVersion < 0 || header.apiVersion > 4) {
                    out.write(buildErrorResponse(header.correlationId, (short) 35));
                } else {
                    out.write(buildSuccessResponse(header.correlationId));
                }
                out.flush();
            } else {
                System.err.println("Unknown api_key " + header.apiKey + ", skipping.");
            }
        }
    }

    /**
     * Runs the server on the specified port. The server listens for incoming 
     * client connections and dispatches them for handling.
     * 
     * @param port the port to listen on.
     * @throws IOException if an I/O error occurs.
     */
    public static void runServer(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.err.println("Server is listening on port " + port);
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
                try {
                    handleClient(clientSocket);
                } finally {
                    clientSocket.close();
                }
            }
        } finally {
            serverSocket.close();
        }
    }

    /**
     * Main entry point.
     * 
     * @param args command-line arguments (not used).
     */
    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");
        try {
            runServer(9092);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
