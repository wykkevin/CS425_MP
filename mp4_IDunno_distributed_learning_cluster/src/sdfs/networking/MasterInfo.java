package sdfs.networking;

import ml.AvailableModel;
import ml.MLQueryData;
import ml.RawMLQueryData;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sdfs.JsonSerializable;
import sdfs.Main;
import sdfs.SdfsFileMetadata;
import sdfs.WorkerQueryPair;

import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static sdfs.networking.UdpServent.TEXT_EXTENSION;
import static sdfs.networking.UdpServent.LOGGER;

/**
 * Class includes fields and methods for file master/ml coordinator.
 */
public class MasterInfo implements JsonSerializable {
    // Use public for most of the fields because they will all be set and get in UdpServent. Save some lines of getters and setters.
    private String masterGroupMemberIp;
    private String hotReplaceGroupMemberIp;

    private ConcurrentHashMap<String, RawMLQueryData> preprocessingQueries = new ConcurrentHashMap<>(); // The key has the format of query1
    private ConcurrentHashMap<AvailableModel, List<MLQueryData>> todoQueries = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, MLQueryData> workerToInProgressQueries = new ConcurrentHashMap<>();
    public List<String> resultFilePaths = new ArrayList<>();
    public int finishedAlexnetQueries = 0;
    public int finishedResnetQueries = 0;
    public List<Double> alexnetRunTimes = new ArrayList<>();
    public List<Double> resnetRunTimes = new ArrayList<>();
    // Only keep time in the last minute
    public List<Long> alexnetFinishTime = new ArrayList<>();
    public List<Long> resnetFinishTime = new ArrayList<>();
    public String routeResultClientIp;
    public long startTime = 0L;

    // These fields only helps make calculation easier. Recalculate these values for new master
    private List<String> freeWorkers = new ArrayList<>();
    private int toDoAlexnetQueries = 0;
    private int toDoResnetQueries = 0;
    private int inProgressAlexnetQueries = 0;
    private int inProgressResnetQueries = 0;

    /**
     * Used in the beginning of ring initialization when there was only one member.
     *
     * @param masterGroupMemberIp the ip of the initial group member.
     */
    public MasterInfo(String masterGroupMemberIp) {
        this.masterGroupMemberIp = masterGroupMemberIp;
    }

    public MasterInfo() {

    }

    public void resetCurrentJobs(List<String> currentMembers) {
        freeWorkers = currentMembers;

        List<MLQueryData> currentTodoAlexNetQueries = todoQueries.getOrDefault(AvailableModel.ALEXNET, new ArrayList<>());
        List<MLQueryData> currentTodoResNetQueries = todoQueries.getOrDefault(AvailableModel.RESNET, new ArrayList<>());
        for (String worker : workerToInProgressQueries.keySet()) {
            MLQueryData queryData = workerToInProgressQueries.get(worker);
            if (queryData.getAvailableModel().equals(AvailableModel.ALEXNET)) {
                currentTodoAlexNetQueries.add(0, queryData);
            } else if (queryData.getAvailableModel().equals(AvailableModel.RESNET)) {
                currentTodoResNetQueries.add(0, queryData);
            }
        }

        workerToInProgressQueries = new ConcurrentHashMap<>();
        todoQueries.put(AvailableModel.ALEXNET, currentTodoAlexNetQueries);
        todoQueries.put(AvailableModel.RESNET, currentTodoResNetQueries);

        toDoAlexnetQueries = currentTodoAlexNetQueries.size();
        toDoResnetQueries = currentTodoResNetQueries.size();
        inProgressAlexnetQueries = 0;
        inProgressResnetQueries = 0;
    }

    /**
     * MasterInfo backup methods
     */
    public JSONObject preprocessingQueriesToJson() {
        JSONObject jsonObject = new JSONObject();
        for (String filename : preprocessingQueries.keySet()) {
            jsonObject.put(filename, preprocessingQueries.get(filename).toJson());
        }
        return jsonObject;
    }

    public void updatePreprocessingQueries(JSONObject jsonObject) {
        preprocessingQueries.clear();
        for (Object key : jsonObject.keySet()) {
            preprocessingQueries.put((String) key, CommandParserUtil.decodeRawMLQueryData((JSONObject) jsonObject.get(key)));
        }
    }

    public JSONObject todoQueriesToJson() {
        JSONObject jsonObject = new JSONObject();
        for (AvailableModel model : todoQueries.keySet()) {
            List<MLQueryData> queryList = todoQueries.get(model);
            JSONArray jsonArray = new JSONArray();
            for (MLQueryData mlQueryData : queryList) {
                jsonArray.add(mlQueryData.toJson());
            }
            jsonObject.put(model.toString(), jsonArray);
        }
        return jsonObject;
    }

    public void updateTodoQueries(JSONObject jsonObject) {
        todoQueries = new ConcurrentHashMap<>();
        for (Object key : jsonObject.keySet()) {
            LOGGER.fine("updateTodoQueries " + key);
            AvailableModel model = AvailableModel.valueOf((String) key);
            List<MLQueryData> newListOfTodoQueries = new ArrayList<>();
            for (Object object : (JSONArray) jsonObject.get(key)) {
                newListOfTodoQueries.add(CommandParserUtil.decodeMLQueryData((JSONObject) object));
            }
            todoQueries.put(model, newListOfTodoQueries);
        }
    }

    public JSONObject workerToInProgressQueriesToJson() {
        JSONObject jsonObject = new JSONObject();
        for (String worker : workerToInProgressQueries.keySet()) {
            jsonObject.put(worker, workerToInProgressQueries.get(worker).toJson());
        }
        return jsonObject;
    }

    public void updateWorkerToInProgressQueriesToJson(JSONObject jsonObject) {
        workerToInProgressQueries = new ConcurrentHashMap<>();
        for (Object key : jsonObject.keySet()) {
            workerToInProgressQueries.put((String) key, CommandParserUtil.decodeMLQueryData((JSONObject) jsonObject.get(key)));
        }
    }

    public static void sendFileMetadataTo(InetAddress targetMemberIp, Map<String, SdfsFileMetadata> fileMetadata) {
        JSONObject messageJsonObject = new JSONObject();
        messageJsonObject.put("command", MessageType.PUT_BACKUP_METADATA.toString());

        JSONObject fileMetadataJsonObject = new JSONObject();
        for (Map.Entry<String, SdfsFileMetadata> fileMetadataEntry : fileMetadata.entrySet()) {
            fileMetadataJsonObject.put(fileMetadataEntry.getKey(), fileMetadataEntry.getValue().toJson());
        }
        messageJsonObject.put("fileMetadata", fileMetadataJsonObject);

        Main.udpServent.sendMessage(messageJsonObject.toJSONString(), targetMemberIp, UdpServent.GROUP_PORT);
    }

    public String getMasterGroupMemberIp() {
        return masterGroupMemberIp;
    }

    public void setMasterGroupMemberIp(String masterGroupMemberIp) {
        this.masterGroupMemberIp = masterGroupMemberIp;
    }

    public String getHotReplaceGroupMemberIp() {
        return hotReplaceGroupMemberIp;
    }

    public void setHotReplaceGroupMemberIp(String hotReplaceGroupMemberIp) {
        this.hotReplaceGroupMemberIp = hotReplaceGroupMemberIp;
    }

    public boolean isMasterMember(GroupMember member) {
        return member.getIp().getHostName().equals(masterGroupMemberIp);
    }

    /**
     * ML related methods
     */
    public List<String> getFreeWorkers() {
        return freeWorkers;
    }

    public ConcurrentHashMap<String, RawMLQueryData> getPreprocessingQueries() {
        return preprocessingQueries;
    }

    public void addNewWorker(String ip) {
        freeWorkers.add(ip);
    }

    public void addRawQuery(String filename, RawMLQueryData rawQuery) {
        preprocessingQueries.put(filename, rawQuery);
    }

    public void handleQueryFile(String queryPath) {
        // queryPath has a format like query1.zip
        String queryName = CommandParserUtil.unzipLocalDirFileAndReturnPureName(queryPath);
        // Get file from LocalDir and separate in batches
        RawMLQueryData rawMLQueryData = preprocessingQueries.get(queryName);
        preprocessingQueries.remove(queryName);
        String localQueryFile = rawMLQueryData.getQueryFile() + TEXT_EXTENSION;
        Long batchSize = rawMLQueryData.getBatchSize();
        AvailableModel model = rawMLQueryData.getModel();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("./LocalDir/" + localQueryFile));
            long batchCount = 0L;
            List<String> inputsInBatch = new ArrayList<>();
            String line;
            List<MLQueryData> newQueries = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                if (batchCount >= batchSize) {
                    newQueries.add(new MLQueryData(inputsInBatch, model, queryName));
                    batchCount = 0L;
                    inputsInBatch = new ArrayList<>();
                }
                inputsInBatch.add(line);
                batchCount++;
            }
            // Add remaining jobs in query
            if (inputsInBatch.size() > 0) {
                newQueries.add(new MLQueryData(inputsInBatch, model, queryName));
            }
            reader.close();

            // Add newQueries to todoQueries
            LOGGER.info("Adding new query " + newQueries.size() + " using model " + model);
            List<MLQueryData> currentQueries = todoQueries.getOrDefault(model, new ArrayList<>());
            currentQueries.addAll(newQueries);
            todoQueries.put(model, currentQueries);
            if (model.equals(AvailableModel.ALEXNET)) {
                toDoAlexnetQueries += newQueries.size();
            } else if (model.equals(AvailableModel.RESNET)) {
                toDoResnetQueries += newQueries.size();
            }

            // Create a blank result file in LocalDir with name <queryName>_<model>_result.txt
            String filePath = queryName + "_" + rawMLQueryData.getModel() + "_result.txt";
            this.resultFilePaths.add(filePath);
            FileWriter fileWriter = new FileWriter("./LocalDir/" + filePath, false);
            fileWriter.write("");
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public WorkerQueryPair updateDataAndGetAssignment() {
        LOGGER.fine("Checking has more task alex " + toDoAlexnetQueries + " res " + toDoResnetQueries + " Free workers are " + freeWorkers);
        LOGGER.fine("Alex in progress " + inProgressAlexnetQueries + " Res in progress " + inProgressResnetQueries);
        boolean hasMoreTask = toDoAlexnetQueries > 0 || toDoResnetQueries > 0;
        if (hasMoreTask && freeWorkers.size() > 0) {
            if (startTime == 0L) {
                startTime = System.currentTimeMillis();
            }
            // Decide next query. Try to balance the job. Run the model that has a lower query processing rate.
            AvailableModel model;
            if (((finishedAlexnetQueries + inProgressAlexnetQueries) >= (finishedResnetQueries + inProgressResnetQueries) && toDoResnetQueries > 0) || toDoAlexnetQueries <= 0) {
                toDoResnetQueries--;
                inProgressResnetQueries++;
                model = AvailableModel.RESNET;
            } else {
                toDoAlexnetQueries--;
                inProgressAlexnetQueries++;
                model = AvailableModel.ALEXNET;
            }
            List<MLQueryData> currentQueries = todoQueries.get(model);
            MLQueryData assignedQuery = currentQueries.remove(0);
            todoQueries.put(model, currentQueries);

            String assignedWorker = freeWorkers.remove(0);
            workerToInProgressQueries.put(assignedWorker, assignedQuery);
            return new WorkerQueryPair(assignedWorker, assignedQuery);
        } else {
            return new WorkerQueryPair(null, null);
        }
    }

    public void handleFinishedQuery(String workerIp, MLQueryData query) throws IOException, InterruptedException {
        LOGGER.info(query.getContentList() + " is finished");
        AvailableModel model = query.getAvailableModel();
        // Store result to local file. File is named as <queryName>_<model>_result.txt
        String filePath = "LocalDir/" + query.getQueryName() + "_" + model + "_result.txt";
        FileWriter fileWriter = new FileWriter(filePath, true);
        fileWriter.append(query.getResult());
        fileWriter.close();

        // Store file to the backup coordinator
        String[] args = new String[]{"sh", "-c", "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null " + filePath + " " + hotReplaceGroupMemberIp + ":" + System.getProperty("user.dir") + "/" + filePath};
        ProcessBuilder builder = new ProcessBuilder(args);
        Process process = builder.start();
        process.waitFor();
        process.destroy();

        // Handle workers
        if (model.equals(AvailableModel.ALEXNET)) {
            inProgressAlexnetQueries--;
            finishedAlexnetQueries++;
            alexnetRunTimes.add(query.getRunTimeInSeconds());
            alexnetFinishTime.add(System.currentTimeMillis());
        } else if (model.equals(AvailableModel.RESNET)) {
            inProgressResnetQueries--;
            finishedResnetQueries++;
            resnetRunTimes.add(query.getRunTimeInSeconds());
            resnetFinishTime.add(System.currentTimeMillis());
        }
        workerToInProgressQueries.remove(workerIp);
        freeWorkers.add(workerIp);
    }

    public List<String> uploadResultFiles() {
        // Upload results when unfinished task is 0.
        List<String> output = new ArrayList<>();
        if (toDoAlexnetQueries == 0 && toDoResnetQueries == 0 && inProgressAlexnetQueries == 0 && inProgressResnetQueries == 0) {
            LOGGER.fine("Upload result files to SDFS");
            output = this.resultFilePaths;
            this.resultFilePaths = new ArrayList<>();
        }
        return output;
    }

    public void handleFailedWorker(String workerIp) {
        freeWorkers.remove(workerIp);
        MLQueryData currentQuery = workerToInProgressQueries.get(workerIp);
        if (currentQuery != null) {
            LOGGER.info("Reassign query " + currentQuery.getContentList() + " model " + currentQuery.getAvailableModel());
            AvailableModel model = currentQuery.getAvailableModel();
            workerToInProgressQueries.remove(workerIp);
            if (model.equals(AvailableModel.ALEXNET)) {
                toDoAlexnetQueries++;
                inProgressAlexnetQueries--;
            } else if (model.equals(AvailableModel.RESNET)) {
                toDoResnetQueries++;
                inProgressResnetQueries--;
            }
            List<MLQueryData> todoModelQueries = todoQueries.getOrDefault(model, new ArrayList<>());
            todoModelQueries.add(0, currentQuery);
            todoQueries.put(model, todoModelQueries);
        }
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("masterGroupMemberIp", masterGroupMemberIp);
        jsonObject.put("hotReplaceGroupMemberIp", hotReplaceGroupMemberIp);
        return jsonObject;
    }

    /**
     * Query stats
     */
    // C1
    public String showQueryRateAndFinishedQueryCount() {
        StringBuilder sb = new StringBuilder();
        long currentTime = System.currentTimeMillis();
        if (startTime != 0L) {
            long timeElapsedInMilliSeconds = currentTime - startTime;
            double timeElapsedInSeconds = timeElapsedInMilliSeconds / 1000.0;
            double alexnetQueriesPerSecond = finishedAlexnetQueries * 1.0 / timeElapsedInSeconds;
            double resnetQueriesPerSecond = finishedResnetQueries * 1.0 / timeElapsedInSeconds;
            sb.append("ALEXNET overall query rate (per second) is " + alexnetQueriesPerSecond + "\n");
            sb.append("RESNET overall query rate (per second) is " + resnetQueriesPerSecond + "\n");
            sb.append("Finished Alexnet query is " + finishedAlexnetQueries + ". Finished Resnet query is " + finishedResnetQueries + ".\n");
            System.out.println("ALEXNET overall query rate (per second) is " + alexnetQueriesPerSecond);
            System.out.println("RESNET overall query rate (per second) is " + resnetQueriesPerSecond);
            System.out.println("Finished Alexnet query is " + finishedAlexnetQueries + ". Finished Resnet query is " + finishedResnetQueries);
        }
        sb.append(showQueryRateForModel(AvailableModel.ALEXNET, currentTime));
        sb.append(showQueryRateForModel(AvailableModel.RESNET, currentTime));
        return sb.toString();
    }

    private StringBuilder showQueryRateForModel(AvailableModel model, long currentTime) {
        StringBuilder sb = new StringBuilder();
        List<Long> finishTimeList;
        if (model.equals(AvailableModel.ALEXNET)) {
            finishTimeList = alexnetFinishTime;
        } else {
            finishTimeList = resnetFinishTime;
        }
        int oneMinuteCount = 0;
        int tenSecondsCount = 0;
        List<Long> newFinishTimeList = new ArrayList<>();
        for (long time : finishTimeList) {
            if (currentTime - time < 60000) {
                oneMinuteCount++;
                newFinishTimeList.add(time);
                if (currentTime - time < 10000) {
                    tenSecondsCount++;
                }
            }
        }
        if (model.equals(AvailableModel.ALEXNET)) {
            alexnetFinishTime = newFinishTimeList;
        } else {
            resnetFinishTime = newFinishTimeList;
        }
        System.out.println(model + " query rate (per second) in the last minute is " + oneMinuteCount / 60.0);
        System.out.println(model + " query rate (per second) in the last 10 seconds is " + tenSecondsCount / 10.0);
        sb.append(model + " query rate (per second) in the last minute is " + oneMinuteCount / 60.0 + "\n");
        sb.append(model + " query rate (per second) in the last 10 seconds is " + tenSecondsCount / 10.0 + "\n");
        return sb;
    }

    // C2
    public String showRunTimeStats() {
        return printRunTimeStats(AvailableModel.ALEXNET) + printRunTimeStats(AvailableModel.RESNET);
    }

    private String printRunTimeStats(AvailableModel model) {
        StringBuilder sb = new StringBuilder();
        List<Double> runTimesList;
        if (model.equals(AvailableModel.ALEXNET)) {
            runTimesList = alexnetRunTimes;
        } else {
            runTimesList = resnetRunTimes;
        }
        Collections.sort(runTimesList);
        int runTimesListSize = runTimesList.size();
        if (runTimesListSize > 0) {
            // Median
            if (runTimesListSize % 2 == 0) {
                double runTimesMedian = ((runTimesList.get(runTimesListSize / 2) + runTimesList.get(runTimesListSize / 2 - 1))) / 2;
                sb.append("Median runtime of " + model + " is " + runTimesMedian + "\n");
            } else {
                double runTimesMedian = runTimesList.get(runTimesListSize / 2);
                sb.append("Median runtime of " + model + " is " + runTimesMedian + "\n");
            }
            // Standard deviation and Average (Mean)
            long sum = 0L;
            double standardDeviation = 0.0;
            for (double runTime : runTimesList) {
                sum += runTime;
            }
            double runTimesMean = sum * 1.0 / runTimesListSize;
            sb.append("Average runtime of " + model + " is " + runTimesMean + "\n");
            for (double runTime : runTimesList) {
                standardDeviation += Math.pow(runTime - runTimesMean, 2);
            }
            double runTimesStandardDeviation = Math.sqrt(standardDeviation / runTimesListSize);
            sb.append("Standard deviation of " + model + " is " + runTimesStandardDeviation + "\n");
            double runTime90 = percentile(runTimesList, 90);
            sb.append("90th percentile of " + model + " is " + runTime90 + "\n");
            double runTime95 = percentile(runTimesList, 95);
            sb.append("95th percentile of " + model + " is " + runTime95 + "\n");
            double runTime99 = percentile(runTimesList, 99);
            sb.append("99th percentile of " + model + " is " + runTime99 + "\n");
        } else {
            sb.append(model + " has no data yet.\n");
        }
        return sb.toString();
    }

    private double percentile(List<Double> runTimes, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * runTimes.size());
        return runTimes.get(index - 1);
    }

    // C5
    public String showVMAssignments() {
        StringBuilder sb = new StringBuilder();
        for (String workerIp : workerToInProgressQueries.keySet()) {
            MLQueryData currentQueryData = workerToInProgressQueries.get(workerIp);
            sb.append("Worker " + workerIp + " is running " + currentQueryData.getAvailableModel() + "\n");
        }
        for (String workerIp : freeWorkers) {
            sb.append("Worker " + workerIp + " is waiting for new query\n");
        }
        return sb.toString();
    }
}
