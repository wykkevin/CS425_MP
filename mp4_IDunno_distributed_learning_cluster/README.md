# MP4 – IDunno, a Distributed Learning Cluster

## Basic Setup
Put any local file under the `LocalDir` directory. Run `sh build.sh` to build and run the client. Please enter `y` if it is the initial introducer when `Are you the first member? (y/n)`
is shown on the screen. The client will print out a log containing the current ip. Then, enter any command listed below.
For all other VMs, enter `n` for the question above and then enter `join` to join the group.

## Start Machine Learning Work
First, before starting the system, the user need to prepare a query file. For example, the query name is `query1`. The user need to store a `query1.zip` 
file under the `LocalDir` directory. The zip file should include a `query1.txt` file and all the inputs. For our system, 
it will be images. The `query1.txt` file should contains the list of filename for the test input files. Each line should 
contain one filename.

# To run the query
Run `store-query query1` to store the zip file into SDFS for all the VMs to use.\
Run `train` to train the neural networks. When the message `Neural network is ready` shows on the screen, the user can add jobs.\
Run `add-job <model> <queryname> <batchSize>` to run the job. Please use `alexnet` or `resnet` for the model field. One 
example command is `add-job alexnet query1 1`

# To show the job statistics
Run `show-query-rate` to show the query rate and finished query count.\
Run `show-query-data` to show the query data like average, std, etc.\
Run `get-query-result` to start printing the query results on the client's console.\
Run `stop-query-result` to stop printing the query results on the client's console.\
Run `show-assignment` to show which model does the VMs running.

## Other Commands
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
