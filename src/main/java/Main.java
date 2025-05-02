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
            // Set SO_REUSEADDR so that we don't get 'Address already in use' errors.
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
                if (bytesRead == -1) {
                    break;
                }
                totalRead += bytesRead;
            }
            
            if (totalRead < 12) {
                System.err.println("Incomplete header received!");
                clientSocket.close();
                serverSocket.close();
                return;
            }
            
            // Unpack the 12 bytes. Set byte order to big-endian.
            ByteBuffer reqBuffer = ByteBuffer.wrap(reqHeader);
            reqBuffer.order(ByteOrder.BIG_ENDIAN);
            int requestMessageSize = reqBuffer.getInt(); // message_size (not used)
            short requestApiKey = reqBuffer.getShort();
            short requestApiVersion = reqBuffer.getShort();
            int correlationId = reqBuffer.getInt();
            System.err.println("Received correlation_id: " + correlationId);
            
            // Build response:
            // - message_size: 4 bytes (any value works; we'll use 0)
            // - correlation_id: parsed from the request header.
            // All integers are 32-bit signed in big-endian order.
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.putInt(0);             // message_size (placeholder)
            buffer.putInt(correlationId);  // correlation_id from request
            byte[] response = buffer.array();
            
            // Send response.
            OutputStream out = clientSocket.getOutputStream();
            out.write(response);
            out.flush();
            // Shutdown the output side to allow the client to read the complete response.
            clientSocket.shutdownOutput();
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.err.println("IOException: " + e.getMessage());
            }
        }
    }
}
