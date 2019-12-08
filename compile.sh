export JAVA_HOME=/usr/local/jdk1.8.0_101
mkdir ./build
javac -cp "./src/main/java/:/opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/*" ./src/main/java/*.java -d ./build -Xlint
jar cvf knnMapReduce.jar -C build/ .