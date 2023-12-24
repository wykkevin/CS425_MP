package sdfs.networking;

public enum MessageType {
    /**
     * Command to ask for grep result
     * Json includes "grep" field
     */
    GREP,
    /**
     * Command to return the grep result
     * Json includes "grepResponse" field
     */
    GREP_RESP,
    /**
     * Command that a joining node sends to the introducer.
     */
    CLIENT_JOIN,
    /**
     * Command that a node in the system sends to the joining node with an IP of itself.
     * Json includes "member" array field
     */
    ALLOW_JOIN,
    /**
     * Command that a joining node sends to join the group followed by its id.
     * Json includes "member" field
     */
    JOIN,
    /**
     * Command that the join receiver sends to the joining node with the information of the current member list.
     * Json includes "memberList" array field, a "master" field, and a "updatedMember" field
     */
    SUCCESS_JOIN,
    /**
     * Command that the join receiver sends to other nodes to share the information of the new node.
     * Json includes "member" field
     */
    SHARE_JOIN,
    /**
     * Command that a node showing it is leaving the group followed by its ip address.
     * Json includes "member" field
     */
    LEAVE,
    /**
     * Command to check if the node is still alive.
     */
    PING,
    /**
     * Command to respond to the PING command
     */
    PONG,
    /**
     * Command to start and proceed an election.
     * Json includes "electingMember" and "initiator" field
     */
    PUT,
    /**
     * Command that respond to the client that wants to put a file
     * Json includes "memberIpString", "localFilePath", "sdfsFilePath", and "version" field
     */
    PUT_LOCATION,
    /**
     * Command that tells a VM that the file has been uploaded to it.
     * Json includes "sdfsFilePath", "neededReplicas", "clientIp", and "version" field
     */
    FILE_UPLOADED,
    /**
     * Command that tells the client that the file has been saved on the VM.
     * Json includes "sdfsFilePath", "storingMemberIp", "clientIp", and "version" field
     */
    FILE_RECEIVED,
    /**
     * Command that sends to the client that the put command is done.
     * Json includes "sdfsFilePath" field.
     */
    PUT_FINISHED,
    /**
     * Command that a node send to master to know which file version should it get.
     * Json includes "sdfsFilePath", "localFilePath", "requestedVersionCount" fields
     */
    GET,
    /**
     * Command that master responds to the client node with information about file version and stored locations.
     * Json includes "sdfsFilePath", "localFilePath", "version", "storingMembers", and "requestedVersionCount" fields
     */
    GET_RESPONSE,
    /**
     * Command that master responds to the client node when trying to get a file that doesn't exist.
     * Json includes "sdfsFilePath" field.
     */
    GET_ERROR,
    /**
     * Command that client sends to a member to request a file from it
     * Json includes "sdfsFilePath", "localFilePath", "sendToLocal", "version", "clientIp", and "requestedVersionCount" fields
     */
    REQUEST_FILE,
    /**
     * Command that a node sends the file to the client
     * Json includes "filePath" and "sendToLocal" field
     */
    FILE_DOWNLOADED,
    /**
     * Command that a node delete a file from SDFS
     * Json includes "sdfsFilePath" field
     */
    DELETE,
    /**
     * Command that tells the client the VMs that has the file it wants to delete
     * Json includes "sdfsFilePath", "latestVersion" field and "ips" array field
     */
    DELETE_TARGET,
    /**
     * Command that from the client to delete the file that is stored.
     * Json includes "sdfsFilePath" and "latestVersion" field
     */
    DELETE_COMMAND,
    /**
     * Command to get the list of all VM where the file is stored.
     * Json includes "sdfsFilePath" field
     */
    LS,
    /**
     * Command to send the response of LS
     * Json includes "sdfsFilePath" and "ips" field
     */
    LS_RESPONSE,
    /**
     * Command to copy all versions of the file from a node to another node
     * Json includes "ips", "sdfsFilePath" and "version" field
     */
    COPY_FILE,
    /**
     * Command to send file metadata to the backup node.
     * Json includes "fileMetadata" field
     */
    PUT_BACKUP_METADATA,
    /**
     * Commands to send ml data to the backup node.
     */
    PUT_RAW_JOB_DATA,
    PUT_NEW_JOB_DATA,
    PUT_IN_PROGRESS_DATA,
    PUT_FINISHED_DATA,
    PUT_ROUTE_CLIENT,
    /**
     * Command to inform other clients that election process has stopped.
     * Json includes no field
     */
    END_ELECTION,

    /**
     * Command to coordinator to let the system start training
     */
    START_TRAINING,
    /**
     * Command to all member to start training
     */
    WORKER_START_TRAINING,
    /**
     * Command to coordinator that training is finished
     */
    TRAINING_FINISHED,
    /**
     * Command to client that training is finished
     */
    ALL_TRAINING_FINISHED,

    /**
     * Command to add self to be the hot replace
     */
    SHARE_NEW_HOT_REPLACE_GROUPMEMBER,
    /**
     * Command to new coordinator to start hot replace process.
     */
    INITIATE_HOT_REPLACE,
    /**
     * Command to broadcast that hot replace has started with new hot replace info.
     */
    HOT_REPLACE,

    /**
     * Command to coordinator to add a new job
     */
    ADD_JOB,
    /**
     * Command to worker to process the query
     */
    PROCESS_QUERY,
    /**
     * Command to coordinator that the worker finishes running the query with result
     */
    QUERY_FINISHED,
    /**
     * Command to also show the result to client
     */
    ROUTE_RESULT,
    /**
     * Commands from client to show details of the ml jobs
     */
    SHOW_QUERY_RATE,
    SHOW_QUERY_DATA,
    GET_QUERY_RESULT,
    STOP_QUERY_RESULT,
    SHOW_ASSIGNMENT,
    /**
     * Commands to response the show requests
     */
    QUERY_RATE_RESULT,
    QUERY_DATA_RESULT,
    ASSIGNMENT_RESULT,

    STUCKED_VM,
    RECOVERED_VM,
}
