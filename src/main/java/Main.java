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
            // Set SO_REUSEADDR to avoid 'Address already in use' errors.
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            clientSocket = serverSocket.accept();
            System.err.println("Connection from " + clientSocket.getRemoteSocketAddress() + " has been established!");
            
            // Read the first 12 bytes from the request:
            // - 4 bytes: message_size (we don't use this value).
            // - Next, request header v2:
            //     INT16 request_api_key (2 bytes)
            //     INT16 request_api_version (2 bytes)
            //     INT32 correlation_id (4 bytes)
            byte[] reqHeader = new byte[12];
            int totalRead = 0;
            InputStream in = clientSocket.getInputStream();
            while (totalRead < 12) {
                int bytesRead = in.read(reqHeader, totalRead, 12 - totalRead);
                if (bytesRead == -1) break;
                totalRead += bytesRead;
            }
            
            if (totalRead < 12) {
                System.err.println("Incomplete header received!");
                clientSocket.close();
                serverSocket.close();
                return;
            }
            
            // Unpack the header in big-endian order: INT32, INT16, INT16, INT32.
            ByteBuffer reqBuffer = ByteBuffer.wrap(reqHeader);
            reqBuffer.order(ByteOrder.BIG_ENDIAN);
            int requestMessageSize = reqBuffer.getInt(); // not used
            short requestApiKey = reqBuffer.getShort();
            short requestApiVersion = reqBuffer.getShort();
            int correlationId = reqBuffer.getInt();
            System.err.println("Received correlation_id: " + correlationId +
                               ", requested api_version: " + requestApiVersion);
            
            // Determine error_code:
            // Broker supports ApiVersions request versions 0 - 4.
            short errorCode = (requestApiVersion < 0 || requestApiVersion > 4) ? (short)35 : (short)0;
            
            // Build ApiVersions response:
            if (errorCode == 0) {
                // For a valid "ApiVersions" request, response body layout is:
                // - error_code: 2 bytes
                // - array length: 4 bytes (we use 1)
                // - One entry (6 bytes):
                //      api_key: 2 bytes (use 18 for ApiVersions)
                //      min_version: 2 bytes (eg., 0)
                //      max_version: 2 bytes (at least 4)
                // Body length = 2 + 4 + 6 = 12 bytes.
                // Response header is 4 bytes (correlation_id).
                // Therefore, message length (first 4 bytes) must be 4 + 12 = 16.
                
                ByteBuffer buffer = ByteBuffer.allocate(20);
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.putInt(16);                // message_size: number of bytes following (4+12)
                buffer.putInt(correlationId);       // correlation_id from the request
                buffer.putShort((short)0);          // error_code = 0 (no error)
                buffer.putInt(1);                 // array length = 1
                buffer.putShort((short)18);         // api_key = 18 (ApiVersions)
                buffer.putShort((short)0);          // min_version = 0
                buffer.putShort((short)4);          // max_version = 4 (at least 4)
                byte[] response = buffer.array();
                
                OutputStream out = clientSocket.getOutputStream();
                out.write(response);
                out.flush();
                clientSocket.shutdownOutput();
            } else {
                // In case of error, send a minimal response containing just the error_code.
                // For simplicity, we use a body with only error_code (2 bytes) and header (4 bytes).
                // Message length = 4 + 2 = 6.
                ByteBuffer buffer = ByteBuffer.allocate(10);
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.putInt(6);                 // message_size = 6
                buffer.putInt(correlationId);       // correlation_id from request
                buffer.putShort(errorCode);         // error_code (35)
                byte[] response = buffer.array();
                
                OutputStream out = clientSocket.getOutputStream();
                out.write(response);
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
