import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Main {
    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");
        
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            clientSocket = serverSocket.accept();
            System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
            
            // Read 12-byte header: 4 bytes message_size (unused) + 2 bytes api_key + 2 bytes api_version + 4 bytes correlation_id
            byte[] reqHeader = new byte[12];
            int totalRead = 0;
            InputStream in = clientSocket.getInputStream();
            while (totalRead < 12) {
                int bytesRead = in.read(reqHeader, totalRead, 12 - totalRead);
                if (bytesRead == -1)
                    break;
                totalRead += bytesRead;
            }
            if (totalRead < 12) {
                System.err.println("Incomplete header received!");
                clientSocket.close();
                serverSocket.close();
                return;
            }
            ByteBuffer reqBuffer = ByteBuffer.wrap(reqHeader);
            reqBuffer.order(ByteOrder.BIG_ENDIAN);
            int unused_messageSize = reqBuffer.getInt(); // not used
            short requestApiKey = reqBuffer.getShort();
            short requestApiVersion = reqBuffer.getShort();
            int correlationId = reqBuffer.getInt();
            System.err.println("Received correlation_id: " + correlationId +
                               ", requested api_version: " + requestApiVersion);
            
            // Determine error code: broker supports ApiVersions v0 to v4.
            short errorCode = (requestApiVersion < 0 || requestApiVersion > 4) ? (short)35 : (short)0;
            
            OutputStream out = clientSocket.getOutputStream();
            
            if (requestApiKey == 18) { // ApiVersions request
                if (errorCode != 0) {
                    // Error response: minimal body
                    // For error, build: error_code (2 bytes) + empty array (compact array length 0) + throttle_time_ms (4 bytes) + empty TAG_BUFFER (1 byte)
                    ByteBuffer buffer = ByteBuffer.allocate(4 + 2 + 1 + 4 + 1);
                    buffer.order(ByteOrder.BIG_ENDIAN);
                    buffer.putInt( (4 + 2 + 1 + 4 + 1) ); // message_length = 12
                    buffer.putInt(correlationId);
                    buffer.putShort(errorCode);
                    buffer.put((byte)0); // compact array: 0 elements (0 means no elements)
                    buffer.putInt(0);    // throttle_time_ms = 0
                    buffer.put((byte)0); // empty response TAG_BUFFER
                    out.write(buffer.array());
                } else {
                    // Successful response using flexible (compact) encoding.
                    // Response body fields:
                    // error_code: 2 bytes
                    // api_keys: compact array. For one element, length = 1+1 = 2 (0x02)
                    //   Entry:
                    //      api_key: INT16 (2 bytes) => 18
                    //      min_version: INT16 (2 bytes) => 0
                    //      max_version: INT16 (2 bytes) => 4
                    //      entry TAG_BUFFER: empty compact bytes (1 byte 0x00)
                    // throttle_time_ms: INT32 (4 bytes, 0)
                    // response TAG_BUFFER: compact bytes (empty, 1 byte 0x00)
                    //
                    // Body length = 2 + 1 + (2+2+2+1) + 4 + 1 = 15 bytes.
                    // Overall message: 4 (correlation_id) + 15 = 19 bytes.
                    
                    ByteBuffer buffer = ByteBuffer.allocate(4 + 15); // 4 bytes for correlation_id not included in message_length field.
                    buffer.order(ByteOrder.BIG_ENDIAN);
                    buffer.putInt(19);                // message_length: total bytes after this field (19)
                    buffer.putInt(correlationId);       // correlation_id
                    buffer.putShort((short)0);          // error_code = 0
                    buffer.put((byte)2);                // compact array length = 2 (1 element + 1)
                    buffer.putShort((short)18);         // api_key = 18
                    buffer.putShort((short)0);          // min_version = 0
                    buffer.putShort((short)4);          // max_version = 4
                    buffer.put((byte)0);                // entry TAG_BUFFER = empty
                    buffer.putInt(0);                   // throttle_time_ms = 0
                    buffer.put((byte)0);                // response TAG_BUFFER = empty
                    out.write(buffer.array());
                }
                out.flush();
                clientSocket.shutdownOutput();
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null)
                    clientSocket.close();
                if (serverSocket != null)
                    serverSocket.close();
            } catch (IOException e) {
                System.err.println("IOException: " + e.getMessage());
            }
        }
    }
}
