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
                    // Error response: minimal body.
                    // Body: error_code (2 bytes) + compact array (1 byte: 0 for no elements) 
                    //       + throttle_time_ms (4 bytes) + response TAG_BUFFER (1 byte)
                    // => Body length = 2 + 1 + 4 + 1 = 8 bytes.
                    // Overall message: 4 bytes (correlation_id) + 8 = 12 bytes.
                    ByteBuffer buffer = ByteBuffer.allocate(4 + 8);
                    buffer.order(ByteOrder.BIG_ENDIAN);
                    buffer.putInt(4 + 8);         // message_length = 12 bytes (correlation_id + body)
                    buffer.putInt(correlationId);   // correlation_id
                    buffer.putShort(errorCode);     // error_code
                    buffer.put((byte)0);            // compact array: no elements
                    buffer.putInt(0);               // throttle_time_ms = 0
                    buffer.put((byte)0);            // empty response TAG_BUFFER
                    out.write(buffer.array());
                } else {
                    // Successful response using flexible (compact) encoding.
                    // Response body fields:
                    // - error_code: 2 bytes (value 0)
                    // - api_keys: compact array:
                    //     Length: unsigned varint (1 byte), for one element equals 1+1 = 2
                    //     Entry: api_key (2 bytes, value 18), 
                    //            min_version (2 bytes, value 0), 
                    //            max_version (2 bytes, value 4),
                    //            entry TAG_BUFFER: 1 byte (empty, 0x00)
                    // - throttle_time_ms: 4 bytes (value 0)
                    // - response TAG_BUFFER: compact bytes (1 byte, 0x00)
                    //
                    // Body length = 2 + 1 + (2+2+2+1) + 4 + 1 = 15 bytes.
                    // Overall message = correlation_id (4 bytes) + body (15 bytes) = 19 bytes.
                    // Total bytes transmitted = message_length field (4) + 19 = 23 bytes.
                    ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 15); // 4 for message_length, 4 for correlation_id, 15 for body.
                    buffer.order(ByteOrder.BIG_ENDIAN);
                    buffer.putInt(4 + 15);               // message_length = 19 bytes (correlation_id + body)
                    buffer.putInt(correlationId);          // correlation_id
                    buffer.putShort((short)0);             // error_code = 0
                    buffer.put((byte)2);                   // compact array length = 2 (one element + 1)
                    buffer.putShort((short)18);            // api_key = 18
                    buffer.putShort((short)0);             // min_version = 0
                    buffer.putShort((short)4);             // max_version = 4
                    buffer.put((byte)0);                   // entry TAG_BUFFER = empty
                    buffer.putInt(0);                      // throttle_time_ms = 0
                    buffer.put((byte)0);                   // response TAG_BUFFER = empty
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
