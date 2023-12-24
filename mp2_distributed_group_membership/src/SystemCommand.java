/**
 * Data class for the commands that we use to send among the nodes.
 */
public class SystemCommand {
    public SystemCommandType systemCommandType;
    public String content;

    public SystemCommand(SystemCommandType systemCommandType, String content) {
        this.systemCommandType = systemCommandType;
        this.content = content;
    }
}

