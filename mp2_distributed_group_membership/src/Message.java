/**
 * Data class for the message that we send among the nodes.
 */
public class Message {
    private String systemCommand;

    public String getSystemCommand() {
        return systemCommand;
    }

    public Message(SystemCommandType systemCommandType, String content) {
        if (content == null) {
            this.systemCommand = systemCommandType.toEncryptedString();
        } else {
            this.systemCommand = systemCommandType.toEncryptedString() + CommandParserUtil.COMMAND_SEPARATOR + content;
        }
    }
}
