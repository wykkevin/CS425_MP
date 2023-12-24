# MP3 – Simple Distributed File System (SDFS)

## Setup
Put any local file under the `LocalDir` directory. Run `sh build.sh` to build and run the client. Please enter `y` if it is the initial introducer when `Are you the first member? (y/n)`
is shown on the screen. The client will print out a log containing the current ip. Then, enter any command listed below.
For all other VMs, enter `n` for the question above and then enter `join` to join the group.

## Commands
`grep <options> <keyword>` : Fetch log entries matching the keyword from every machine that joined the group. Option `-E` will enable regex as keyword and `-c` will replace the entries with word count. `-Ec` will do both.\
`list_mem` : List the current membership list of the group.\
`list_self` : Show the IP address of the current machine with a join timestamp. \
`join` : Join the group.\
`leave` : Leave the group in a peaceful way.\
`put <localFilePath> <sdfsFilePath>`: put the file that is stored in `LocalDir` folder to SDFS. For example, for the file1.txt, we should do `put file1.txt sdfsFileName.txt`\
`get <sdfsFilePath> <localFilePath>`: get the file from SDFS and store it in `LocalDir`.\
`delete <sdfsFilePath>`: delete the file with the name that is stored in SDFS.\
`store`: get a list for files that are stored in the current node in SDFS.\
`ls <sdfsFilePath>`: get a list of nodes that are storing the given SDFS file.\
`get-versions <sdfsFilePath> <versions> <localFilePath>`: get the last `<version>` number of files with the name that are stored in SDFS and store it in `LocalDir`.\
`linux-command <linuxCommand>`: run the linux command locally like `more` and `diff`.
