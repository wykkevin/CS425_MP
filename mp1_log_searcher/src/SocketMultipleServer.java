import java.io.IOException;
import java.net.ServerSocket;

/**
 * Main class as a server. This class will be able to handle multiple client
 * connections.
 */
public class SocketMultipleServer {

    public static void main(String[] args) {
        SocketMultipleServer server = new SocketMultipleServer();
        server.start();
    }

    public void start() {
        try {
            // Port hardcoded to 8001.
            ServerSocket serverSocket = new ServerSocket(8001);
            System.out.println("Listening for a connection");
            while (true) {
                new SocketServer(serverSocket.accept()).start();
                System.out.println("Received a connection ");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}