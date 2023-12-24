import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Server class. It will receive the message from client and handle the command.
 * The command will be run as a shell command directly.
 */
public class SocketServer extends Thread {
    // Assume the log file is always in that location and has the same extension
    private static final String LOG_DIRECTORY = "./logFiles/";
    private static final String LOG_FILE_EXTENSION = ".log";

    private final Socket clientSocket;

    public SocketServer(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream());

            // Check log file in the folder. Add the path text to the beginning of each line
            // if there is only one file. File names will be added when there are multiple files.
            String singleFilePath = getSingleFilePath();
            QueryHandler queryHandler = new QueryHandler();

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                long start = System.currentTimeMillis();

                List<String> commandResults = queryHandler.getQueryResults(inputLine, singleFilePath);
                // First, tell the client the number of lines will be printed.
                out.println(commandResults.size());
                for (String commandResult : commandResults) {
                    out.println(commandResult);
                }
                out.flush();

                long finish = System.currentTimeMillis();
                long timeElapsed = finish - start;
                System.out.println("Server takes " + timeElapsed + " milliseconds to process the command");
            }
            System.out.println("Connection closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Return the file path if there is only one file with expected format under
    // the directory. Otherwise, return null.
    private String getSingleFilePath() {
        File[] allFiles = new File(LOG_DIRECTORY).listFiles();
        List<String> output = new ArrayList<>();
        if (allFiles != null) {
            for (File file : allFiles) {
                if (file.getName().endsWith(LOG_FILE_EXTENSION)) {
                    output.add(file.getPath());
                }
            }
        }
        if (output.size() == 1) {
            return output.get(0);
        }
        return null;
    }
}