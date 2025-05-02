import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

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

            // Build response:
            // message_size: 4 bytes (any value works; we'll use 0)
            // correlation_id: hard-coded to 7
            // All integers are 32-bit signed in big-endian order.
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(0);  // message_size (placeholder value)
            buffer.putInt(7);  // correlation_id
            byte[] response = buffer.array();

            // Send response.
            OutputStream out = clientSocket.getOutputStream();
            out.write(response);
            out.flush();
            // Shutdown the output side to allow the client to read the complete response.
            clientSocket.shutdownOutput();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
