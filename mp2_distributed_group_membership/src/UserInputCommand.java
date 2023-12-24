/**
 * Data class for the commands that we get from user input
 */
public class UserInputCommand {
    public UserInputCommandType userInputCommandType;
    public String content;

    public UserInputCommand(UserInputCommandType userInputCommandType, String content) {
        this.userInputCommandType = userInputCommandType;
        this.content = content;
    }
}
