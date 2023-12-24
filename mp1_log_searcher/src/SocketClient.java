import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Client class. It will connect to a server using port 8001, send the message,
 * and print out the responses received from server.
 */
public class SocketClient {
    private PrintStream out;
    private BufferedReader in;
    Socket socket;
    private final String serverIp;

    public SocketClient(String serverIp) {
        this.serverIp = serverIp;
    }

    public boolean start() {
        System.out.println("Connecting to: " + serverIp);
        try {
            socket = new Socket(serverIp, 8001);
            out = new PrintStream(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            return true;
        } catch (Exception e) {
            // We don't print stack trace to keep the console clean.
            // It is expected to get an Exception when the server is off.
            System.out.println("Failed to connect to " + serverIp);
            return false;
        }
    }

    public List<String> sendMessage(String message) {
        out.println(message);
        List<String> responses = new ArrayList<>();
        try {
            String singleLineResponse = in.readLine();
            // singleLineResponse will be null if the server is disconnected.
            if (singleLineResponse != null) {
                // We will know how many lines to read based on this information.
                int outputLineCount = Integer.parseInt(singleLineResponse);
                for (int i = 0; i < outputLineCount; i++) {
                    singleLineResponse = in.readLine();
                    responses.add(singleLineResponse);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return responses;
    }
}
