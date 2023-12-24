package sdfs;

public enum GetFileType {
    NORMAL,
    QUERY_FILE, // Coordinator get this to process the input jobs
    TEST_INPUT, // Worker get this to process the query
}
