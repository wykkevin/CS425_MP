public enum SystemCommandType {
    GREP, // Command to ask for grep result
    GREP_RESP, // Command to return the grep result
    CLIENT_JOIN, // Command that a joining node sends to the introducer.
    ROUTE_JOIN, // Command that the introducer sends to the joining node with an IP of a node in the group.
    JOIN, // Command that a joining node sends to join the group followed by its id.
    SUCCESS_JOIN, // Command that the join receiver sends to the joining node with the information of the current member list.
    SHARE_JOIN, // Command that the join receiver sends to other nodes to share the information of the new node.
    LEAVE, // Command that a node showing it is leaving the group followed by its ip address.
    PING, // Command to check if the node is still alive.
    PONG; // Command to respond to the PING command

    public String toEncryptedString() {
        return this.toString();
    }
}
