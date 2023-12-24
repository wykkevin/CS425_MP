# MP2-Distributed Group Membership

## Setup
run `sh build.sh` to build and run the client. Please enter `01` if it is `VM01` when `Which VM is this?` is shown on the
screen. Use `0` for the drop rate when we don't want the message to be dropped. The client will print out a log containing
the current ip. Then, enter any command listed below. For all VMs other than `VM01`, enter `join` to join the group.

## Commands
`grep <options> <keyword>` : Fetch log entries matching the keyword from every machine that joined the group. Option `-E` will enable regex as keyword and `-c` will replace the entries with word count. `-Ec` will do both.\
`list_mem` : List the current membership list of the group.\
`list_self` : Show the IP address of the current machine with a join timestamp. \
`join` : Join the group.\
`leave` : Leave the group in a peaceful way.
