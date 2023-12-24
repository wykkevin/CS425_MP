import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

/**
 * This class generate a log file. We will use the file for testing.
 */
public class LogGenerator {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("fileName expectedWord expectedWordCount TotalLines");
        String query = scanner.nextLine();
        scanner.close();
        // Since this class is only used for testing, we didn't add error checks.
        String[] inputs = query.split(" ");
        String fileName = inputs[0];
        String expectedWord = inputs[1];
        int expectedWordCount = Integer.parseInt(inputs[2]);
        int totalLines = Integer.parseInt(inputs[3]);

        generate(fileName, expectedWord, expectedWordCount, totalLines);
    }

    public static void generate(String fileName, String expectedWord, int expectedWordCount, int totalLines) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + ".log"));
            for (int i = 0; i < expectedWordCount; i++) {
                writer.write(expectedWord + generateRandomString() + "\n");
            }
            for (int i = expectedWordCount; i < totalLines; i++) {
                writer.write(generateRandomString() + "\n");
            }
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String generateRandomString() {
        int leftLimit = 33; // '!'
        int rightLimit = 126; // '~'
        int targetStringLength = new Random().nextInt(9) + 1;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }
}
