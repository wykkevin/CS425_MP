import org.junit.jupiter.api.*;

import java.io.File;
import java.util.List;

/**
 * Unit tests for querying with grep search on local files. Need to make sure that the logFiles folder is empty before
 * running the tests.
 */
public class UnitTests {

    private static String getLogsGeneratePath(String fileName) {
        return "./logFiles/" + (fileName.length() > 0 ? "__test_" + fileName : fileName);
    }

    private static QueryHandler queryHandler;

    @BeforeAll
    public static void setUp() {
        queryHandler = new QueryHandler();
    }

    @AfterEach
    public void tearDown() {
        for (File file : new File(getLogsGeneratePath("")).listFiles()) {
            if (file.getName().startsWith("__test_")) file.delete();
        }
    }

    /**
     * Tests for result that shows in a single file
     */
    @Test
    public void test_singleFile_noOccurrence() {
        System.out.println(getLogsGeneratePath("test1"));
        LogGenerator.generate(getLogsGeneratePath("test1"), "aaaa", 1, 1);
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", "__test_test1.log");
        Assertions.assertEquals(1, queryResults.size());

        String result = queryResults.get(0);
        Assertions.assertTrue(result.startsWith("__test_test1.log"));
        String[] resultSplit = result.split(":");
        Assertions.assertEquals(0, Integer.parseInt(resultSplit[1]));
    }

    @Test
    public void test_singleFile_singleResult_rowCount() {
        LogGenerator.generate(getLogsGeneratePath("test1"), "test", 1, 1);
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", "__test_test1.log");
        Assertions.assertEquals(1, queryResults.size());

        String result = queryResults.get(0);
        Assertions.assertTrue(result.startsWith("__test_test1.log"));
        String[] resultSplit = result.split(":");
        Assertions.assertEquals(1, Integer.parseInt(resultSplit[1]));

    }

    @Test
    public void test_singleFile_singleResult_rowCount_regex() {
        LogGenerator.generate(getLogsGeneratePath("test1"), "test", 1, 1);
        List<String> queryResults = queryHandler.getQueryResults("grep -Ec ^te.t", "__test_test1.log");
        Assertions.assertEquals(1, queryResults.size());

        String result = queryResults.get(0);
        Assertions.assertTrue(result.startsWith("__test_test1.log"));
        String[] resultSplit = result.split(":");
        Assertions.assertEquals(1, Integer.parseInt(resultSplit[1]));

    }

    @Test
    public void test_singleFile_singleResult_eachLine_regex() {
        LogGenerator.generate(getLogsGeneratePath("test1"), "keyword", 1, 1);
        List<String> queryResults = queryHandler.getQueryResults("grep -E ^key..rd", "__test_test1.log");
        Assertions.assertEquals(1, queryResults.size());

        String result = queryResults.get(0);
        Assertions.assertTrue(result.startsWith("__test_test1.log"));
        Assertions.assertTrue(result.contains("keyword"));
    }

    @Test
    public void test_singleFile_singleResult_eachLine() {
        LogGenerator.generate(getLogsGeneratePath("test1"), "keyword", 1, 1);
        List<String> queryResults = queryHandler.getQueryResults("grep keyword", "__test_test1.log");
        Assertions.assertEquals(1, queryResults.size());

        String result = queryResults.get(0);
        Assertions.assertTrue(result.startsWith("__test_test1.log"));
        Assertions.assertTrue(result.contains("keyword"));
    }

    @Test
    public void test_singleFile_multipleResult_rowCount() {
        LogGenerator.generate(getLogsGeneratePath("test1"), "test", 50, 100);
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", "__test_test1.log");

        String result = queryResults.get(0);
        Assertions.assertTrue(result.startsWith("__test_test1.log"));
        String[] resultSplit = result.split(":");
        Assertions.assertEquals(50, Integer.parseInt(resultSplit[1]));
    }

    @Test
    public void test_singleFile_withMultipleFiles_rowCount() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", (i == 0) ? 50 : 0, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals((i == 0) ? 50 : 0, Integer.parseInt(resultSplit[1]));
        }
    }

    /**
     * Tests for result that shows in some of the files
     */
    @Test
    public void test_someFile_lowFrequency_rowCount() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", (i % 2 == 0) ? 10 : 0, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals((i % 2 == 0) ? 10 : 0, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_someFile_mediumFrequency_rowCount() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", (i % 2 == 0) ? 50 : 0, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals((i % 2 == 0) ? 50 : 0, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_someFile_highFrequency_rowCount() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", (i % 2 == 0) ? 90 : 0, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals((i % 2 == 0) ? 90 : 0, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_someFile_allMatches_rowCount() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", (i % 2 == 0) ? 100 : 0, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals((i % 2 == 0) ? 100 : 0, Integer.parseInt(resultSplit[1]));
        }
    }

    /**
     * Tests for result that shows in all the files
     */
    @Test
    public void test_allFile_noMatches_rowCount() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", 0, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals(0, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_multipleFile_lowFrequency_rowCount_regex() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", 10, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -Ec ^te.t", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals(10, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_allFile_mediumFrequency_rowCount() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", 50, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -c test", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals(50, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_multipleFile_mediumFrequency_rowCount_regex() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", 50, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -Ec ^te.t", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals(50, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_multipleFile_highFrequency_rowCount_regex() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", 80, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -Ec ^te.t", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals(80, Integer.parseInt(resultSplit[1]));
        }
    }

    @Test
    public void test_multipleFile_allMatches_rowCount_regex() {
        for (int i = 0; i < 10; i++) {
            LogGenerator.generate(getLogsGeneratePath("test" + i), "test", 100, 100);
        }
        List<String> queryResults = queryHandler.getQueryResults("grep -Ec ^te.t", null);

        for (int i = 0; i < queryResults.size(); i++) {
            String result = queryResults.get(i);

            Assertions.assertTrue(result.contains("__test_test" + i + ".log"));
            String[] resultSplit = result.split(":");
            Assertions.assertEquals(100, Integer.parseInt(resultSplit[1]));
        }
    }
}