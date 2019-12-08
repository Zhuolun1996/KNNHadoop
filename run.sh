export JAVA_HOME=/usr/local/jdk1.8.0_101
rm ./knnData/output*
hadoop fs -rm -r knnData/output1
hadoop fs -rm -r knnData/output2
hadoop fs -rm -r knnData/output3
hadoop fs -rm -r knnData/output4
hadoop fs -mkdir knnData
hadoop fs -mkdir knnData/input
hadoop fs -put ./knnData/input/test.txt knnData/input/
hadoop jar knnMapReduce.jar KnnMapReduce "$@"
sh ./getResult.sh 1
sh ./getResult.sh 2
sh ./getResult.sh 3
sh ./getResult.sh 4