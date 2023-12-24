import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Use a process to run the grep command directly.
 */
public class QueryHandler {
    private static final String LOG_FILE_LOCATION = " ./logFiles/*.log";

    /**
     * Query local files on server using grep system call.
     * @param inputLine The grep command received from client
     * @param singleFilePath If the directory ./logFiles only contains 1 log, this will be set to the path of the file.
     * @return Grep results from stdout collected in a list.
     */
    public List<String> getQueryResults(String inputLine, String singleFilePath) {
        List<String> commandResults = new ArrayList<>();
        try {
            // We need to treat it as shell for command that includes *.
            // https://stackoverflow.com/questions/2111983/java-runtime-getruntime-exec-wildcards
            String[] args = new String[]{"sh", "-c", inputLine + LOG_FILE_LOCATION};
            ProcessBuilder builder = new ProcessBuilder(args);
            Process process = builder.start();
            BufferedReader responseReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String commandOutput;
            while ((commandOutput = responseReader.readLine()) != null) {
                if (singleFilePath == null) {
                    commandResults.add(commandOutput);
                } else {
                    commandResults.add(singleFilePath + ":" + commandOutput);
                }
            }
            process.waitFor();
            process.destroy();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return commandResults;
    }
}
