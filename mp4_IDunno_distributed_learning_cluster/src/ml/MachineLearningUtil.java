package ml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import static sdfs.networking.UdpServent.LOGGER;

public class MachineLearningUtil {
    private static String FILE_SEPARATOR = "@";

    public void trainNeuralNetworks() {
        String[] alexArguments = new String[]{"sh", "-c", "python3 ./src/ml/train_alexnet.py"};
        runMLScript(alexArguments);

        String[] resnetArguments = new String[]{"sh", "-c", "python3 ./src/ml/train_resnet.py"};
        runMLScript(resnetArguments);

        String[] modelClassesArguments = new String[]{"sh", "-c", "cp ./LocalDir/model_classes.txt ./Db/model_classes.txt"};
        runMLScript(modelClassesArguments);
    }

    public String runModel(MLQueryData mlQueryData) {
        AvailableModel model = mlQueryData.getAvailableModel();
        List<String> contentList = mlQueryData.getContentList();
        String content = splitListIntoString(contentList); // Convert the list to a "@" splited string
        String result = "";
        if (model.equals(AvailableModel.ALEXNET)) {
            result = runAlexnet(content);
        } else if (model.equals(AvailableModel.RESNET)) {
            result = runResnet(content);
        }
        return result;
    }

    public String runAlexnet(String filePaths) {
        // Use a "@" splited list of files as input
        String[] arguments = new String[]{"sh", "-c", "python3 ./src/ml/run_alexnet.py " + filePaths};
        return runMLScript(arguments);
    }

    public String runResnet(String filePaths) {
        String[] arguments = new String[]{"sh", "-c", "python3 ./src/ml/run_resnet.py " + filePaths};
        return runMLScript(arguments);
    }

    private String runMLScript(String[] arguments) {
        LOGGER.fine("start runMLScript " + arguments[2]);
        String result = "";
        try {
            ProcessBuilder builder = new ProcessBuilder(arguments);
            Process process = builder.start();
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String errorOutput;
            while ((errorOutput = errorReader.readLine()) != null) {
                System.out.println(errorOutput);
            }
            BufferedReader responseReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String commandOutput;
            while ((commandOutput = responseReader.readLine()) != null) {
                System.out.println(commandOutput);
                result += commandOutput + "\n";
            }
            process.waitFor();
            process.destroy();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public String splitListIntoString(List<String> files) {
        StringBuilder sb = new StringBuilder();
        for (String file : files) {
            sb.append(file);
            sb.append(FILE_SEPARATOR);
        }
        String filePaths = sb.toString();
        return filePaths.substring(0, filePaths.length() - 1);
    }
}
