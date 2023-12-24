package sdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Use a process to run the grep command directly. Copied from MP1.
 */
public class GrepQueryHandler {
    private static final String LOG_FILE_LOCATION = " ./logFiles/*.log";
    private static final String LOG_DIRECTORY = "./logFiles/";
    private static final String LOG_FILE_EXTENSION = ".log";

    /**
     * Query local files on server using grep system call.
     *
     * @param inputLine The grep command received from client
     * @return Grep results from stdout collected in a list.
     */
    public static List<String> getQueryResults(String inputLine) {
        List<String> commandResults = new ArrayList<>();
        String singleFilePath = getSingleFilePath();
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

    private static String getSingleFilePath() {
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
