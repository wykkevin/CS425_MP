# Clean up the directory and prepare for compiling the project
rm -f sources.txt
find ./src -name '*.java' > sources.txt
rm -rf ./compile
mkdir ./compile
# Compile the project, need to add "-target 8" for the VMs for compatibility.
javac -target 8 -source 8 -d ./compile -classpath ./compile/mp3.jar:./lib/json-simple-1.1.1.jar @sources.txt

java -classpath ./compile:./compile/mp3.jar:./lib/json-simple-1.1.1.jar sdfs/Main