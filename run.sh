export JAVA_HOME=/usr/local/jdk1.8.0_101
rm ./knnData/output1/output
rm ./knnData/result
rm ./knnData/output*
hadoop fs -rm -r knnData/output1
hadoop fs -rm -r knnData/output2
hadoop fs -rm -r knnData/output3
hadoop fs -rm -r knnData/output4
hadoop fs -mkdir knnData
hadoop fs -mkdir knnData/input
hadoop fs -put ./knnData/input/test.txt knnData/input/
hadoop jar knnMapReduce.jar KnnMapReduce "$@"