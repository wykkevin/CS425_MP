# cs425-Team05-Yuankai-Boyou

## MP1 Log Searcher
Run `sh startServer.sh` on all the machines to initiate the server. Then on the client you want to use, open a different
terminal and run `sh startClient.sh` as the client to type the query. Once the client side is started, you will see `$ `
in the console and you can type the grep query. <br><br>
We assume all the log files are stored under the `logFiles`
folder and have the extension `.log`. Therefore, you will need to put the files you want to search for in that location.
The query syntax will be similar to the system call `grep`. <br><br>
To return the lines that contain the keyword in the files: `grep <keyword>`.<br>
To return the total number of lines that contain the keyword in the files: `grep -c <keyword>`.<br>
To return the lines that contain the regular expression pattern in the files: `grep -E <regex>`.<br>
To return the total number of lines that contain the regular expression pattern in the files: `grep -Ec <regex>`.<br><br>
We will also print out the time used for the command and the total count if the row count is requested. 
