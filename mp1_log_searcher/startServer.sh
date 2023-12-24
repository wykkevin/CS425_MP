# Clean up the directory and prepare for compiling the project
rm -f sources.txt
find ./src -name '*.java' > sources.txt
rm -rf ./compile
mkdir ./compile
# Compile the project, need to add "-target 8" for the VMs for compatibility.
javac -target 8 -source 8 -d ./compile -classpath ./compile/mp1.jar @sources.txt
# Start the server
java -classpath ./compile:./compile/mp1.jar SocketMultipleServer